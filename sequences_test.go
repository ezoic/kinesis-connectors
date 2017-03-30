package connector

import (
	"sort"
	"testing"

	"github.com/shopspring/decimal"
)

func Test_SequencesList(t *testing.T) {

	s := make(SequenceList, 0)

	// put the first element into the list
	s = s.Append("123")
	if len(s) != 1 {
		t.Fatalf("expected len(s)=1, got %v", len(s))
	}
	if s.SequenceExists("123") == false {
		t.Fatalf("expected abc to exist in sequence list")
	}
	if s.SequenceExists("999") == true {
		t.Fatalf("expected lll not to exist in sequence list")
	}

	// put the sequence into the list again, it shouldn't actually ad it
	s = s.Append("123")
	if len(s) != 1 {
		t.Fatalf("expected len(s)=1, got %v", len(s))
	}
	if s.SequenceExists("123") == false {
		t.Fatalf("expected abc to exist in sequence list")
	}
	if s.SequenceExists("999") == true {
		t.Fatalf("expected lll not to exist in sequence list")
	}

	// put an element into the list that would go to the front of the array
	s = s.Append("121")
	if len(s) != 2 {
		t.Fatalf("expected len(s)=1, got %v", len(s))
	}
	if s.SequenceExists("123") == false {
		t.Fatalf("expected abc to exist in sequence list")
	}
	if s.SequenceExists("121") == false {
		t.Fatalf("expected aba to exist in sequence list")
	}
	if s.SequenceExists("999") == true {
		t.Fatalf("expected lll not to exist in sequence list")
	}
	if s[0].Cmp(decimal.New(121, 0)) != 0 || s[1].Cmp(decimal.New(123, 0)) != 0 {
		t.Fatalf("bad list: %+v", s)
	}
	if sort.IsSorted(s) == false {
		t.Fatalf("not sorted, uh oh")
	}

	// put an element into the list that would go to the end of the array
	s = s.Append("124")
	if len(s) != 2 {
		t.Fatalf("expected len(s)=2, got %v", len(s))
	}
	if s.SequenceExists("123") == false {
		t.Fatalf("expected abc to exist in sequence list")
	}
	if s.SequenceExists("121") == false {
		t.Fatalf("expected aba to exist in sequence list")
	}
	if s.SequenceExists("124") == false {
		t.Fatalf("expected abd to exist in sequence list")
	}
	if s.SequenceExists("999") == true {
		t.Fatalf("expected lll not to exist in sequence list")
	}
	if s[0].Cmp(decimal.New(121, 0)) != 0 || s[1].Cmp(decimal.New(124, 0)) != 0 {
		t.Fatalf("bad list: %+v", s)
	}
	if sort.IsSorted(s) == false {
		t.Fatalf("not sorted, uh oh")
	}

	// put an element into the list that would go to the middle of the array
	s = s.Append("122")
	if len(s) != 2 {
		t.Fatalf("expected len(s)=1, got %v", len(s))
	}
	if s.SequenceExists("123") == false {
		t.Fatalf("expected abc to exist in sequence list")
	}
	if s.SequenceExists("121") == false {
		t.Fatalf("expected aba to exist in sequence list")
	}
	if s.SequenceExists("124") == false {
		t.Fatalf("expected abd to exist in sequence list")
	}
	if s.SequenceExists("122") == false {
		t.Fatalf("expected abb to exist in sequence list")
	}
	if s.SequenceExists("999") == true {
		t.Fatalf("expected lll not to exist in sequence list")
	}
	if s[0].Cmp(decimal.New(121, 0)) != 0 || s[1].Cmp(decimal.New(124, 0)) != 0 {
		t.Fatalf("bad list: %+v", s)
	}
	if sort.IsSorted(s) == false {
		t.Fatalf("not sorted, uh oh")
	}

	// put an element into the list that would go at the beginning
	s = s.Append("120")
	if len(s) != 2 {
		t.Fatalf("expected len(s)=1, got %v", len(s))
	}
	if s.SequenceExists("123") == false {
		t.Fatalf("expected abc to exist in sequence list")
	}
	if s.SequenceExists("121") == false {
		t.Fatalf("expected aba to exist in sequence list")
	}
	if s.SequenceExists("124") == false {
		t.Fatalf("expected abd to exist in sequence list")
	}
	if s.SequenceExists("122") == false {
		t.Fatalf("expected abb to exist in sequence list")
	}
	if s.SequenceExists("120") == false {
		t.Fatalf("expected abb to exist in sequence list")
	}
	if s.SequenceExists("999") == true {
		t.Fatalf("expected lll not to exist in sequence list")
	}
	if s[0].Cmp(decimal.New(120, 0)) != 0 || s[1].Cmp(decimal.New(124, 0)) != 0 {
		t.Fatalf("bad list: %+v", s)
	}
	if sort.IsSorted(s) == false {
		t.Fatalf("not sorted, uh oh")
	}

	// put an element into the list that would go at the beginning
	s = s.Append("129")
	if len(s) != 2 {
		t.Fatalf("expected len(s)=1, got %v", len(s))
	}
	if s.SequenceExists("123") == false {
		t.Fatalf("expected abc to exist in sequence list")
	}
	if s.SequenceExists("121") == false {
		t.Fatalf("expected aba to exist in sequence list")
	}
	if s.SequenceExists("124") == false {
		t.Fatalf("expected abd to exist in sequence list")
	}
	if s.SequenceExists("122") == false {
		t.Fatalf("expected abb to exist in sequence list")
	}
	if s.SequenceExists("120") == false {
		t.Fatalf("expected abb to exist in sequence list")
	}
	if s.SequenceExists("129") == false {
		t.Fatalf("expected abb to exist in sequence list")
	}
	if s.SequenceExists("999") == true {
		t.Fatalf("expected lll not to exist in sequence list")
	}
	if s[0].Cmp(decimal.New(120, 0)) != 0 || s[1].Cmp(decimal.New(129, 0)) != 0 {
		t.Fatalf("bad list: %+v", s)
	}
	if sort.IsSorted(s) == false {
		t.Fatalf("not sorted, uh oh")
	}
}
