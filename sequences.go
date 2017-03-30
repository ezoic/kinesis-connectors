package connector

import (
	"sort"

	l4g "github.com/ezoic/log4go"
	"github.com/shopspring/decimal"
)

type SequenceList []decimal.Decimal

func (s SequenceList) Len() int {
	return len(s)
}
func (s SequenceList) Less(i, j int) bool {
	return s[i].Cmp(s[j]) < 0
}
func (s SequenceList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SequenceList) Append(v string) SequenceList {
	vd, err := decimal.NewFromString(v)
	if err != nil {
		l4g.Warn("cannot convert %s to decimal: %s", v, err.Error())
		return s
	}

	fi := sort.Search(len(s), func(i int) bool {
		return s[i].Cmp(vd) >= 0
	})
	if fi < len(s) && s[fi].Cmp(vd) == 0 {
		// we found the element, do nothing
	} else {
		s = append(s, vd)
		copy(s[fi+1:], s[fi:])
		s[fi] = vd

		if len(s) > 2 {
			s[1] = s[len(s)-1]
			s = s[:2]
		}
	}
	return s
}

func (s SequenceList) SequenceExists(v string) bool {
	vd, err := decimal.NewFromString(v)
	if err != nil {
		l4g.Warn("cannot convert %s to decimal: %s", v, err.Error())
		return true
	}

	fi := sort.Search(len(s), func(i int) bool {
		return s[i].Cmp(vd) >= 0
	})

	inList := false
	if fi == len(s) {
		inList = false
	} else if fi == 0 && s[0].Cmp(vd) != 0 {
		inList = false
	} else {
		inList = true
	}

	return inList
}
