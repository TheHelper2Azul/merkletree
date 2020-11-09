package merkletree

import (
	"bytes"
	"crypto/sha256"
	"testing"
)

func TestCalculateHash(t *testing.T) {
	content1 := []byte("")
	// content2 := []byte()
	// content3 := []byte()

	bucket1 := NewBucket(uint64(16), "test")
	h := sha256.New()
	h.Write(content1)
	hash1 := h.Sum(nil)

	tables := []struct {
		bucket *Bucket
		hash   []byte
	}{
		{bucket1, hash1},
		// {bucket2, hash2},
		// {bucket3, hash3},
	}
	for _, table := range tables {
		value, _ := table.bucket.CalculateHash()
		if bytes.Compare(value, table.hash) != 0 {
			t.Errorf("Value of %v was incorrect, got: %v, want: %v.", table.bucket, value, table.hash)
		}
	}
}

// func TestContainsDay(t *testing.T) {
// 	date1, _ := time.Parse("2006-01-02", "2020-04-01")
// 	date2, _ := time.Parse("2006-01-02 15:04:05", "2020-04-18 14:22:55")
// 	date3, _ := time.Parse("2006-01-02", "2020-04-18")

// 	dates1 := []time.Time{date1, date2}
// 	dates2 := []time.Time{date1}
// 	dates3 := []time.Time{}

// 	tables := []struct {
// 		dates []time.Time
// 		date  time.Time
// 		value bool
// 	}{
// 		{dates1, date1, true},
// 		{dates1, date3, true},
// 		{dates2, date2, false},
// 		{dates3, date3, false},
// 	}
// 	for _, table := range tables {
// 		value := ContainsDay(table.dates, table.date)
// 		if value != table.value {
// 			t.Errorf("Slice %v contains date %v. Got %v but want %v.", table.dates, table.date, value, table.value)
// 		}
// 	}
// }

// func TestSameDays(t *testing.T) {
// 	date1, _ := time.Parse("2006-01-02", "2020-04-01")
// 	date2, _ := time.Parse("2006-01-02 15:04:05", "2020-04-18 14:22:55")
// 	date3, _ := time.Parse("2006-01-02 15:04:05", "2020-04-18 02:22:55")
// 	date4, _ := time.Parse("2006-01-02", "2020-04-18")

// 	tables := []struct {
// 		date1 time.Time
// 		date2 time.Time
// 		value bool
// 	}{
// 		{date1, date2, false},
// 		{date2, date3, true},
// 		{date2, date4, true},
// 		{date4, date3, true},
// 	}
// 	for _, table := range tables {
// 		value := SameDays(table.date1, table.date2)
// 		if value != table.value {
// 			t.Errorf("Got %v and %v are the same day: %v, but should be %v.", table.date1, table.date2, value, table.value)
// 		}
// 	}
// }

// func TestCountDays(t *testing.T) {
// 	date1, _ := time.Parse("2006-01-02", "2020-04-01")
// 	date2, _ := time.Parse("2006-01-02 15:04:05", "2020-04-10 14:22:55")
// 	date3, _ := time.Parse("2006-01-02", "2020-04-02")
// 	date4, _ := time.Parse("2006-01-02", "2020-04-03")

// 	tables := []struct {
// 		dateInit  time.Time
// 		dateFinal time.Time
// 		business  bool
// 		days      int
// 		err       error
// 	}{
// 		{date1, date2, true, 7, nil},
// 		{date1, date2, false, 9, nil},
// 		{date1, date3, true, 1, nil},
// 		{date1, date3, false, 1, nil},
// 		{date1, date4, false, 2, nil},
// 		{date1, date4, true, 2, nil},
// 	}
// 	for _, table := range tables {
// 		value, err := CountDays(table.dateInit, table.dateFinal, table.business)
// 		if value != table.days {
// 			t.Errorf("Number of days bewteen %v and %v is %v but should be %v, as business is %v.", table.dateInit, table.dateFinal, value, table.days, table.business)
// 		}
// 		if err != nil {
// 			t.Errorf("Error should be %v but is %v", table.err, err)
// 		}
// 	}
// }

// func TestGetYesterday(t *testing.T) {
// 	date1 := "2020-01-01"
// 	date2 := "2020-04-18 14:22:55"

// 	layout1 := "2006-01-02"
// 	layout2 := "2006-01-02 15:04:05"

// 	tables := []struct {
// 		date      string
// 		layout    string
// 		yesterday string
// 	}{
// 		{date1, layout1, "2019-12-31"},
// 		{date2, layout2, "2020-04-17 14:22:55"},
// 	}
// 	for _, table := range tables {
// 		value := GetYesterday(table.date, table.layout)
// 		if value != table.yesterday {
// 			t.Errorf("Value of %s was incorrect, got: %v, want: %v.", table.date, value, table.yesterday)
// 		}
// 	}
// }
