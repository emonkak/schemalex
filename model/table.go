package model

import (
	"strings"
)

// NewTable create a new table with the given name
func NewTable(name string) Table {
	return &table{
		name:              name,
		columnNameToIndex: make(map[string]int),
	}
}

func (t *table) ID() string {
	return "table#" + t.name
}

func (t *table) lookupColumnOrderNoLock(id string) (int, bool) {
	idx, ok := t.columnNameToIndex[id]
	return idx, ok
}

func (t *table) LookupColumn(id string) (TableColumn, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	idx, ok := t.lookupColumnOrderNoLock(id)
	if !ok {
		return nil, false
	}

	return t.columns[idx], true
}

func (t *table) LookupColumnOrder(id string) (int, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	idx, ok := t.lookupColumnOrderNoLock(id)
	return idx, ok
}

func (t *table) LookupColumnBefore(id string) (TableColumn, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	idx, ok := t.columnNameToIndex[id]
	if !ok || idx == 0 {
		return nil, false
	}

	return t.columns[idx-1], true
}

func (t *table) LookupIndex(id string) (Index, bool) {
	for idx := range t.Indexes() {
		if idx.ID() == id {
			return idx, true
		}
	}
	return nil, false
}

func (t *table) AddColumn(v TableColumn) Table {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Avoid adding the same TableColumn to multiple tables
	if tblID := v.TableID(); tblID != "" {
		v = v.Clone()
	}
	v.SetTableID(t.ID())
	t.columns = append(t.columns, v)
	t.columnNameToIndex[v.ID()] = len(t.columns) - 1
	return t
}

func (t *table) AddIndex(v Index) Table {
	t.indexes = append(t.indexes, v)
	return t
}

func (t *table) AddOption(v TableOption) Table {
	t.options = append(t.options, v)
	return t
}

func (t *table) Name() string {
	return t.name
}

func (t *table) IsIfNotExists() bool {
	return t.ifnotexists
}

func (t *table) IsTemporary() bool {
	return t.temporary
}

func (t *table) SetIfNotExists(v bool) Table {
	t.ifnotexists = v
	return t
}

func (t *table) HasLikeTable() bool {
	return t.likeTable.Valid
}

func (t *table) SetLikeTable(s string) Table {
	t.likeTable.Valid = true
	t.likeTable.Value = s
	return t
}

func (t *table) LikeTable() string {
	return t.likeTable.Value
}

func (t *table) SetTemporary(v bool) Table {
	t.temporary = v
	return t
}

func (t *table) Columns() chan TableColumn {
	ch := make(chan TableColumn, len(t.columns))
	for _, col := range t.columns {
		ch <- col
	}
	close(ch)
	return ch
}

func (t *table) Indexes() chan Index {
	ch := make(chan Index, len(t.indexes))
	for _, idx := range t.indexes {
		ch <- idx
	}
	close(ch)
	return ch
}

func (t *table) Options() chan TableOption {
	ch := make(chan TableOption, len(t.options))
	for _, idx := range t.options {
		ch <- idx
	}
	close(ch)
	return ch
}

func (t *table) Normalize() (Table, bool) {
	var clone bool
	var additionalIndexes []Index
	var columns []TableColumn
	var defaultCharacterSet string
	var defaultCollation string

	for opt := range t.Options() {
		switch (strings.ToUpper(opt.Key())) {
		case "DEFAULT CHARACTER SET":
			defaultCharacterSet = opt.Value()
		case "DEFAULT COLLATE":
			defaultCollation = opt.Value()
		}
	}

	for col := range t.Columns() {
		ncol, modified := col.Normalize()
		if modified {
			clone = true
		}

		// column_definition [UNIQUE [KEY] | [PRIMARY] KEY]
		// they mean same as INDEX or CONSTRAINT
		switch {
		case ncol.IsPrimary():
			// we have to move off the index declaration from the
			// primary key column to an index associated with the table
			index := NewIndex(IndexKindPrimaryKey, t.ID())
			index.SetType(IndexTypeNone)
			idxCol := NewIndexColumn(ncol.Name())
			index.AddColumns(idxCol)
			additionalIndexes = append(additionalIndexes, index)
			if !modified {
				clone = true
			}
			ncol = ncol.Clone()
			ncol.SetPrimary(false)
		case ncol.IsUnique():
			index := NewIndex(IndexKindUnique, t.ID())
			// if you do not assign a name, the index is assigned the same name as the first indexed column
			index.SetName(ncol.Name())
			index.SetType(IndexTypeNone)
			idxCol := NewIndexColumn(ncol.Name())
			index.AddColumns(idxCol)
			additionalIndexes = append(additionalIndexes, index)
			if !modified {
				clone = true
			}
			ncol = ncol.Clone()
			ncol.SetUnique(false)
		}

		switch ncol.Type() {
		case ColumnTypeChar, ColumnTypeVarChar, ColumnTypeTinyText, ColumnTypeText, ColumnTypeMediumText, ColumnTypeLongText:
			if !ncol.HasCharacterSet() {
				if defaultCharacterSet != "" {
					ncol.SetCharacterSet(defaultCharacterSet)
				}
			}

			if !ncol.HasCollation() {
				if ncol.HasCharacterSet() {
					if ncol.CharacterSet() == defaultCharacterSet && defaultCollation != "" {
						ncol.SetCollation(defaultCollation)
					} else if collation := getDefaultCollationForCharacterSet(ncol.CharacterSet()); collation != "" {
						ncol.SetCollation(collation)
					}
				} else if defaultCollation != "" {
					ncol.SetCollation(defaultCollation)
				}
			}
		}

		columns = append(columns, ncol)
	}

	var indexes []Index
	var seen = make(map[string]struct{})
	for idx := range t.Indexes() {
		nidx, modified := idx.Normalize()
		if modified {
			clone = true
		}
		indexes = append(indexes, nidx)
		seen[nidx.Name()] = struct{}{}
	}

	if !clone {
		return t, false
	}

	tbl := NewTable(t.Name())
	tbl.SetIfNotExists(t.IsIfNotExists())
	tbl.SetTemporary(t.IsTemporary())

	for _, index := range additionalIndexes {
		tbl.AddIndex(index)
	}

	for _, col := range columns {
		tbl.AddColumn(col)
	}

	for _, idx := range indexes {
		tbl.AddIndex(idx)
	}

	for opt := range t.Options() {
		tbl.AddOption(opt)
	}
	return tbl, true
}

// NewTableOption creates a new table option with the given name, value, and a flag indicating if quoting is necessary
func NewTableOption(k, v string, q bool) TableOption {
	return &tableopt{
		key:        k,
		value:      v,
		needQuotes: q,
	}
}

func (t *tableopt) ID() string       { return "tableopt#" + t.key }
func (t *tableopt) Key() string      { return t.key }
func (t *tableopt) Value() string    { return t.value }
func (t *tableopt) NeedQuotes() bool { return t.needQuotes }

func getDefaultCollationForCharacterSet(characterSet string) string {
	switch characterSet {
	case "big5":
		return "big5_chinese_ci"
	case "dec8":
		return "dec8_swedish_ci"
	case "cp850":
		return "cp850_general_ci"
	case "hp8":
		return "hp8_english_ci"
	case "koi8r":
		return "koi8r_general_ci"
	case "latin1":
		return "latin1_swedish_ci"
	case "latin2":
		return "latin2_general_ci"
	case "swe7":
		return "swe7_swedish_ci"
	case "ascii":
		return "ascii_general_ci"
	case "ujis":
		return "ujis_japanese_ci"
	case "sjis":
		return "sjis_japanese_ci"
	case "hebrew":
		return "hebrew_general_ci"
	case "tis620":
		return "tis620_thai_ci"
	case "euckr":
		return "euckr_korean_ci"
	case "koi8u":
		return "koi8u_general_ci"
	case "gb2312":
		return "gb2312_chinese_ci"
	case "greek":
		return "greek_general_ci"
	case "cp1250":
		return "cp1250_general_ci"
	case "gbk":
		return "gbk_chinese_ci"
	case "latin5":
		return "latin5_turkish_ci"
	case "armscii8":
		return "armscii8_general_ci"
	case "utf8":
		return "utf8_general_ci"
	case "ucs2":
		return "ucs2_general_ci"
	case "cp866":
		return "cp866_general_ci"
	case "keybcs2":
		return "keybcs2_general_ci"
	case "macce":
		return "macce_general_ci"
	case "macroman":
		return "macroman_general_ci"
	case "cp852":
		return "cp852_general_ci"
	case "latin7":
		return "latin7_general_ci"
	case "utf8mb4":
		return "utf8mb4_general_ci"
	case "cp1251":
		return "cp1251_general_ci"
	case "utf16":
		return "utf16_general_ci"
	case "utf16le":
		return "utf16le_general_ci"
	case "cp1256":
		return "cp1256_general_ci"
	case "cp1257":
		return "cp1257_general_ci"
	case "utf32":
		return "utf32_general_ci"
	case "binary":
		return "binary"
	case "geostd8":
		return "geostd8_general_ci"
	case "cp932":
		return "cp932_japanese_ci"
	case "eucjpms":
		return "eucjpms_japanese_ci"
	case "gb18030":
		return "gb18030_chinese_ci"
	default:
		return ""
	}
}
