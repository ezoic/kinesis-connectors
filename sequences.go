package connector

import "sort"

type SequenceList []*string

func (s SequenceList) Len() int {
	return len(s)
}
func (s SequenceList) Less(i, j int) bool {
	return *(s[i]) < *(s[j])
}
func (s SequenceList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SequenceList) Append(v string) SequenceList {
	fi := sort.Search(len(s), func(i int) bool {
		return *(s[i]) >= v
	})
	if fi < len(s) && *(s[fi]) == v {
		// we found the element, do nothing
	} else {
		s = append(s, &v)
		copy(s[fi+1:], s[fi:])
		s[fi] = &v
	}
	return s
}

func (s SequenceList) SequenceExists(v string) bool {
	fi := sort.Search(len(s), func(i int) bool {
		return *(s[i]) >= v

	})
	inList := false
	if fi < len(s) && *(s[fi]) == v {
		inList = true
	}
	return inList
}
