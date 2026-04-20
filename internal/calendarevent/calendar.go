package calendarevent

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode"
)

type DateTimeValue struct {
	Value  int
	Repeat int
}

type CalendarEvent struct {
	Weekdays []time.Weekday
	Years    []DateTimeValue
	Months   []DateTimeValue
	Days     []DateTimeValue
	Hours    []DateTimeValue
	Minutes  []DateTimeValue
	Seconds  []DateTimeValue
}

func Parse(input string) (*CalendarEvent, error) {
	s := strings.TrimSpace(strings.ToLower(input))

	if ev, ok := parseNamedSchedule(s); ok {
		return ev, nil
	}

	ev := &CalendarEvent{}
	parts := splitParts(s)

	if len(parts) == 0 {
		return nil, fmt.Errorf("empty calendar event")
	}

	idx := 0

	if isWeekdayPart(parts[idx]) {
		wds, err := parseWeekdays(parts[idx])
		if err != nil {
			return nil, err
		}
		ev.Weekdays = wds
		idx++
	}

	if idx < len(parts) && isDatePart(parts[idx]) {
		err := parseDatePart(parts[idx], ev)
		if err != nil {
			return nil, err
		}
		idx++
	}

	if idx < len(parts) && isTimePart(parts[idx]) {
		err := parseTimePart(parts[idx], ev)
		if err != nil {
			return nil, err
		}
		idx++
	}

	if idx < len(parts) {
		return nil, fmt.Errorf("unexpected trailing content: %q", parts[idx])
	}

	if ev.Weekdays == nil && ev.Years == nil && ev.Months == nil &&
		ev.Days == nil && ev.Hours == nil && ev.Minutes == nil &&
		ev.Seconds == nil {
		return nil, fmt.Errorf("calendar event must have at least one of weekday, date, or time")
	}

	return ev, nil
}

func parseNamedSchedule(s string) (*CalendarEvent, bool) {
	switch s {
	case "minutely":
		return &CalendarEvent{
			Seconds: []DateTimeValue{{Value: 0}},
		}, true
	case "hourly":
		return &CalendarEvent{
			Minutes: []DateTimeValue{{Value: 0}},
			Seconds: []DateTimeValue{{Value: 0}},
		}, true
	case "daily":
		return &CalendarEvent{
			Hours:   []DateTimeValue{{Value: 0}},
			Minutes: []DateTimeValue{{Value: 0}},
			Seconds: []DateTimeValue{{Value: 0}},
		}, true
	case "weekly":
		return &CalendarEvent{
			Weekdays: []time.Weekday{time.Monday},
			Hours:    []DateTimeValue{{Value: 0}},
			Minutes:  []DateTimeValue{{Value: 0}},
			Seconds:  []DateTimeValue{{Value: 0}},
		}, true
	case "monthly":
		return &CalendarEvent{
			Days:    []DateTimeValue{{Value: 1}},
			Hours:   []DateTimeValue{{Value: 0}},
			Minutes: []DateTimeValue{{Value: 0}},
			Seconds: []DateTimeValue{{Value: 0}},
		}, true
	case "yearly", "annually":
		return &CalendarEvent{
			Months:  []DateTimeValue{{Value: 1}},
			Days:    []DateTimeValue{{Value: 1}},
			Hours:   []DateTimeValue{{Value: 0}},
			Minutes: []DateTimeValue{{Value: 0}},
			Seconds: []DateTimeValue{{Value: 0}},
		}, true
	case "quarterly":
		return &CalendarEvent{
			Months:  []DateTimeValue{{Value: 1}, {Value: 4}, {Value: 7}, {Value: 10}},
			Days:    []DateTimeValue{{Value: 1}},
			Hours:   []DateTimeValue{{Value: 0}},
			Minutes: []DateTimeValue{{Value: 0}},
			Seconds: []DateTimeValue{{Value: 0}},
		}, true
	case "semiannually", "semi-annually":
		return &CalendarEvent{
			Months:  []DateTimeValue{{Value: 1}, {Value: 7}},
			Days:    []DateTimeValue{{Value: 1}},
			Hours:   []DateTimeValue{{Value: 0}},
			Minutes: []DateTimeValue{{Value: 0}},
			Seconds: []DateTimeValue{{Value: 0}},
		}, true
	}
	return nil, false
}

func splitParts(s string) []string {
	var parts []string
	current := strings.Builder{}
	for _, r := range s {
		if r == ' ' || r == '\t' {
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
		} else {
			current.WriteRune(r)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}

var weekdayMap = map[string]time.Weekday{
	"mon": time.Monday,
	"tue": time.Tuesday,
	"wed": time.Wednesday,
	"thu": time.Thursday,
	"fri": time.Friday,
	"sat": time.Saturday,
	"sun": time.Sunday,
}

func isWeekdayToken(s string) bool {
	_, ok := weekdayMap[s]
	return ok
}

func isWeekdayPart(s string) bool {
	tokens := strings.SplitSeq(s, ",")
	for tok := range tokens {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		if strings.Contains(tok, "..") {
			rangeParts := strings.SplitN(tok, "..", 2)
			if isWeekdayToken(rangeParts[0]) && isWeekdayToken(rangeParts[1]) {
				continue
			}
			return false
		}
		if !isWeekdayToken(tok) {
			return false
		}
	}
	return true
}

func isDatePart(s string) bool {
	return strings.Contains(s, "-")
}

func isTimePart(s string) bool {
	return strings.Contains(s, ":") || isAllDigits(s)
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

func parseWeekdays(s string) ([]time.Weekday, error) {
	seen := map[time.Weekday]bool{}
	tokens := strings.SplitSeq(s, ",")
	for tok := range tokens {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		if strings.Contains(tok, "..") {
			rangeParts := strings.SplitN(tok, "..", 2)
			start, ok1 := weekdayMap[rangeParts[0]]
			end, ok2 := weekdayMap[rangeParts[1]]
			if !ok1 || !ok2 {
				return nil, fmt.Errorf("invalid weekday range: %q", tok)
			}
			startInt := weekdayToInt(start)
			endInt := weekdayToInt(end)
			if startInt > endInt {
				for i := startInt; i <= 6; i++ {
					seen[intToWeekday(i)] = true
				}
				for i := 0; i <= endInt; i++ {
					seen[intToWeekday(i)] = true
				}
			} else {
				for i := startInt; i <= endInt; i++ {
					seen[intToWeekday(i)] = true
				}
			}
		} else {
			wd, ok := weekdayMap[tok]
			if !ok {
				return nil, fmt.Errorf("invalid weekday: %q", tok)
			}
			seen[wd] = true
		}
	}
	var result []time.Weekday
	for wd := range seen {
		result = append(result, wd)
	}
	sort.Slice(result, func(i, j int) bool {
		return weekdayToInt(result[i]) < weekdayToInt(result[j])
	})
	return result, nil
}

func weekdayToInt(wd time.Weekday) int {
	switch wd {
	case time.Monday:
		return 0
	case time.Tuesday:
		return 1
	case time.Wednesday:
		return 2
	case time.Thursday:
		return 3
	case time.Friday:
		return 4
	case time.Saturday:
		return 5
	case time.Sunday:
		return 6
	}
	return -1
}

func intToWeekday(i int) time.Weekday {
	switch i {
	case 0:
		return time.Monday
	case 1:
		return time.Tuesday
	case 2:
		return time.Wednesday
	case 3:
		return time.Thursday
	case 4:
		return time.Friday
	case 5:
		return time.Saturday
	case 6:
		return time.Sunday
	}
	return time.Monday
}

func parseDatePart(s string, ev *CalendarEvent) error {
	segments := strings.Split(s, "-")

	switch len(segments) {
	case 2:
		months, err := parseValueList(segments[0])
		if err != nil {
			return fmt.Errorf("invalid months: %w", err)
		}
		days, err := parseValueList(segments[1])
		if err != nil {
			return fmt.Errorf("invalid days: %w", err)
		}
		ev.Months = months
		ev.Days = days
	case 3:
		years, err := parseValueList(segments[0])
		if err != nil {
			return fmt.Errorf("invalid years: %w", err)
		}
		months, err := parseValueList(segments[1])
		if err != nil {
			return fmt.Errorf("invalid months: %w", err)
		}
		days, err := parseValueList(segments[2])
		if err != nil {
			return fmt.Errorf("invalid days: %w", err)
		}
		ev.Years = years
		ev.Months = months
		ev.Days = days
	default:
		return fmt.Errorf("invalid date part: %q", s)
	}

	return nil
}

func parseTimePart(s string, ev *CalendarEvent) error {
	segments := strings.Split(s, ":")

	switch len(segments) {
	case 2:
		hours, err := parseValueList(segments[0])
		if err != nil {
			return fmt.Errorf("invalid hours: %w", err)
		}
		minutes, err := parseValueList(segments[1])
		if err != nil {
			return fmt.Errorf("invalid minutes: %w", err)
		}
		ev.Hours = hours
		ev.Minutes = minutes
		if ev.Seconds == nil {
			ev.Seconds = []DateTimeValue{{Value: 0}}
		}
	case 3:
		hours, err := parseValueList(segments[0])
		if err != nil {
			return fmt.Errorf("invalid hours: %w", err)
		}
		minutes, err := parseValueList(segments[1])
		if err != nil {
			return fmt.Errorf("invalid minutes: %w", err)
		}
		seconds, err := parseValueList(segments[2])
		if err != nil {
			return fmt.Errorf("invalid seconds: %w", err)
		}
		ev.Hours = hours
		ev.Minutes = minutes
		ev.Seconds = seconds
	default:
		return fmt.Errorf("invalid time part: %q", s)
	}

	return nil
}

func parseValueList(s string) ([]DateTimeValue, error) {
	if s == "*" {
		return nil, nil
	}

	tokens := strings.Split(s, ",")
	var result []DateTimeValue
	for _, tok := range tokens {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		vals, err := parseSingleValue(tok)
		if err != nil {
			return nil, err
		}
		result = append(result, vals...)
	}
	return result, nil
}

func parseSingleValue(tok string) ([]DateTimeValue, error) {
	if strings.Contains(tok, "..") {
		rangeParts := strings.SplitN(tok, "..", 2)
		start, err := parseInt(rangeParts[0])
		if err != nil {
			return nil, fmt.Errorf("invalid range start: %w", err)
		}
		end, err := parseInt(rangeParts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid range end: %w", err)
		}
		if start > end {
			return nil, fmt.Errorf("range start %d > end %d", start, end)
		}
		var result []DateTimeValue
		for i := start; i <= end; i++ {
			result = append(result, DateTimeValue{Value: i})
		}
		return result, nil
	}

	if strings.Contains(tok, "/") {
		repParts := strings.SplitN(tok, "/", 2)
		startStr := repParts[0]
		repeatStr := repParts[1]

		var start int
		var err error
		if startStr == "*" {
			start = 0
		} else {
			start, err = parseInt(startStr)
			if err != nil {
				return nil, fmt.Errorf("invalid repetition start: %w", err)
			}
		}
		repeat, err := parseInt(repeatStr)
		if err != nil {
			return nil, fmt.Errorf("invalid repetition interval: %w", err)
		}
		if repeat <= 0 {
			return nil, fmt.Errorf("repetition interval must be > 0")
		}
		return []DateTimeValue{{Value: start, Repeat: repeat}}, nil
	}

	val, err := parseInt(tok)
	if err != nil {
		return nil, err
	}
	return []DateTimeValue{{Value: val}}, nil
}

func parseInt(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty integer")
	}
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("invalid integer: %q", s)
		}
		n = n*10 + int(r-'0')
	}
	return n, nil
}

func matchesWeekday(weekdays []time.Weekday, wd time.Weekday) bool {
	if weekdays == nil {
		return true
	}
	return slices.Contains(weekdays, wd)
}

func nextFieldValue(specs []DateTimeValue, current int, min int, max int) (int, bool) {
	if specs == nil {
		if current >= min && current <= max {
			return current, false
		}
		if current < min {
			return min, false
		}
		return min, true
	}

	best := -1
	bestWrap := -1

	for _, spec := range specs {
		if spec.Repeat > 0 {
			start := spec.Value
			if start < min {
				start = min + (spec.Repeat-((min-spec.Value)%spec.Repeat))%spec.Repeat
			}
			for v := start; v <= max; v += spec.Repeat {
				if v >= current {
					if best == -1 || v < best {
						best = v
					}
					break
				}
			}
			for v := spec.Value; v <= max; v += spec.Repeat {
				if v >= min {
					if bestWrap == -1 || v < bestWrap {
						bestWrap = v
					}
					break
				}
			}
		} else {
			v := spec.Value
			if v >= min && v <= max {
				if v >= current {
					if best == -1 || v < best {
						best = v
					}
				}
				if bestWrap == -1 || v < bestWrap {
					bestWrap = v
				}
			}
		}
	}

	if best != -1 {
		return best, false
	}
	if bestWrap != -1 {
		return bestWrap, true
	}
	return min, true
}

func daysInMonth(year int, month time.Month) int {
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

func ComputeNextEvent(ev *CalendarEvent, after time.Time, loc *time.Location) (time.Time, error) {
	if loc == nil {
		loc = time.Local
	}

	t := after.In(loc).Add(time.Second)
	t = t.Truncate(time.Second)

	const maxIterations = 1_000_000

	for range maxIterations {
		year := t.Year()
		month := int(t.Month())
		day := t.Day()
		hour := t.Hour()
		minute := t.Minute()
		second := t.Second()

		nextYear, yearWrapped := nextFieldValue(ev.Years, year, 1970, 2200)
		if yearWrapped {
			return time.Time{}, fmt.Errorf("no matching year found")
		}
		if nextYear > year {
			t = time.Date(nextYear, 1, 1, 0, 0, 0, 0, loc)
			continue
		}

		nextMonth, monthWrapped := nextFieldValue(ev.Months, month, 1, 12)
		if monthWrapped {
			t = time.Date(year+1, 1, 1, 0, 0, 0, 0, loc)
			continue
		}
		if nextMonth > month {
			t = time.Date(year, time.Month(nextMonth), 1, 0, 0, 0, 0, loc)
			continue
		}

		maxDay := daysInMonth(year, time.Month(month))
		nextDay, dayWrapped := nextFieldValue(ev.Days, day, 1, maxDay)
		if dayWrapped {
			t = time.Date(year, time.Month(month)+1, 1, 0, 0, 0, 0, loc)
			continue
		}
		if nextDay > day {
			t = time.Date(year, time.Month(month), nextDay, 0, 0, 0, 0, loc)
			if !matchesWeekday(ev.Weekdays, t.Weekday()) {
				t = time.Date(year, time.Month(month), nextDay+1, 0, 0, 0, 0, loc)
				continue
			}
			continue
		}

		candidate := time.Date(year, time.Month(month), day, hour, minute, second, 0, loc)
		if !matchesWeekday(ev.Weekdays, candidate.Weekday()) {
			t = time.Date(year, time.Month(month), day+1, 0, 0, 0, 0, loc)
			continue
		}

		nextHour, hourWrapped := nextFieldValue(ev.Hours, hour, 0, 23)
		if hourWrapped {
			t = time.Date(year, time.Month(month), day+1, 0, 0, 0, 0, loc)
			continue
		}
		if nextHour > hour {
			t = time.Date(year, time.Month(month), day, nextHour, 0, 0, 0, loc)
			continue
		}

		nextMinute, minuteWrapped := nextFieldValue(ev.Minutes, minute, 0, 59)
		if minuteWrapped {
			t = time.Date(year, time.Month(month), day, hour+1, 0, 0, 0, loc)
			continue
		}
		if nextMinute > minute {
			t = time.Date(year, time.Month(month), day, hour, nextMinute, 0, 0, loc)
			continue
		}

		nextSecond, secondWrapped := nextFieldValue(ev.Seconds, second, 0, 59)
		if secondWrapped {
			t = time.Date(year, time.Month(month), day, hour, minute+1, 0, 0, loc)
			continue
		}

		result := time.Date(year, time.Month(month), day, nextHour, nextMinute, nextSecond, 0, loc)
		return result, nil
	}

	return time.Time{}, fmt.Errorf("could not compute next event within iteration limit")
}
