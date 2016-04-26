package connector

import (
	"sort"
	"testing"
)

func Test_SequencesList(t *testing.T) {

	s := make(SequenceList, 0)

	// put the first element into the list
	s = s.Append("abc")
	if len(s) != 1 {
		t.Fatalf("expected len(s)=1, got %v", len(s))
	}
	if s.SequenceExists("abc") == false {
		t.Fatalf("expected abc to exist in sequence list")
	}
	if s.SequenceExists("lll") == true {
		t.Fatalf("expected lll not to exist in sequence list")
	}

	// put the sequence into the list again, it shouldn't actually ad it
	s = s.Append("abc")
	if len(s) != 1 {
		t.Fatalf("expected len(s)=1, got %v", len(s))
	}
	if s.SequenceExists("abc") == false {
		t.Fatalf("expected abc to exist in sequence list")
	}
	if s.SequenceExists("lll") == true {
		t.Fatalf("expected lll not to exist in sequence list")
	}

	// put an element into the list that would go to the front of the array
	s = s.Append("aba")
	if len(s) != 2 {
		t.Fatalf("expected len(s)=1, got %v", len(s))
	}
	if s.SequenceExists("abc") == false {
		t.Fatalf("expected abc to exist in sequence list")
	}
	if s.SequenceExists("aba") == false {
		t.Fatalf("expected aba to exist in sequence list")
	}
	if s.SequenceExists("lll") == true {
		t.Fatalf("expected lll not to exist in sequence list")
	}
	if *(s[0]) != "aba" || *(s[1]) != "abc" {
		t.Fatalf("bad list: %+v", s)
	}
	if sort.IsSorted(s) == false {
		t.Fatalf("not sorted, uh oh")
	}

	// put an element into the list that would go to the end of the array
	s = s.Append("abd")
	if len(s) != 3 {
		t.Fatalf("expected len(s)=1, got %v", len(s))
	}
	if s.SequenceExists("abc") == false {
		t.Fatalf("expected abc to exist in sequence list")
	}
	if s.SequenceExists("aba") == false {
		t.Fatalf("expected aba to exist in sequence list")
	}
	if s.SequenceExists("abd") == false {
		t.Fatalf("expected abd to exist in sequence list")
	}
	if s.SequenceExists("lll") == true {
		t.Fatalf("expected lll not to exist in sequence list")
	}
	if *(s[0]) != "aba" || *(s[1]) != "abc" || *(s[2]) != "abd" {
		t.Fatalf("bad list: %+v", s)
	}
	if sort.IsSorted(s) == false {
		t.Fatalf("not sorted, uh oh")
	}

	// put an element into the list that would go to the middle of the array
	s = s.Append("abb")
	if len(s) != 4 {
		t.Fatalf("expected len(s)=1, got %v", len(s))
	}
	if s.SequenceExists("abc") == false {
		t.Fatalf("expected abc to exist in sequence list")
	}
	if s.SequenceExists("aba") == false {
		t.Fatalf("expected aba to exist in sequence list")
	}
	if s.SequenceExists("abd") == false {
		t.Fatalf("expected abd to exist in sequence list")
	}
	if s.SequenceExists("abb") == false {
		t.Fatalf("expected abb to exist in sequence list")
	}
	if s.SequenceExists("lll") == true {
		t.Fatalf("expected lll not to exist in sequence list")
	}
	if *(s[0]) != "aba" || *(s[1]) != "abb" || *(s[2]) != "abc" || *(s[3]) != "abd" {
		t.Fatalf("bad list: %+v", s)
	}
	if sort.IsSorted(s) == false {
		t.Fatalf("not sorted, uh oh")
	}
}
