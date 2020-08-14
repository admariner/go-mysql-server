package function

import (
	"math"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression/function/aggregation"
)

func init() {
	// This is to prevent import cycles. Since the sql package cannot reference the function package, we have to
	// push all of the built in functions to a global that will be referenced from within the sql package. Otherwise,
	// we'd need to duplicate a lot of code or split up sql into smaller packages. If a better way is found, then feel
	// free to replace this.
	err := sql.ColumnDefaultManager.AllowedDefaultFunctions.Register(Defaults...)
	if err != nil {
		panic(err) // this should never happen
	}
}

func connIDFuncLogic(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	return ctx.ID(), nil
}

func userFuncLogic(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	return ctx.Client().User, nil
}

// Defaults is the function map with all the default functions.
var Defaults = []sql.Function{
	// elt, find_in_set, insert, load_file, locate
	sql.Function1{Name: "abs", Fn: NewAbsVal},
	NewUnaryFunc("acos", sql.Float64, ACosFunc),
	sql.Function1{Name: "array_length", Fn: NewArrayLength},
	NewUnaryFunc("ascii", sql.Uint8, AsciiFunc),
	NewUnaryFunc("asin", sql.Float64, ASinFunc),
	NewUnaryFunc("atan", sql.Float64, ATanFunc),
	sql.Function1{Name: "avg", Fn: func(e sql.Expression) sql.Expression { return aggregation.NewAvg(e) }},
	NewUnaryFunc("bin", sql.Text, BinFunc),
	NewUnaryFunc("bit_length", sql.Int32, BinFunc),
	sql.Function1{Name: "ceil", Fn: NewCeil},
	sql.Function1{Name: "ceiling", Fn: NewCeil},
	sql.Function1{Name: "char_length", Fn: NewCharLength},
	sql.Function1{Name: "character_length", Fn: NewCharLength},
	sql.FunctionN{Name: "coalesce", Fn: NewCoalesce},
	sql.FunctionN{Name: "concat", Fn: NewConcat},
	sql.FunctionN{Name: "concat_ws", Fn: NewConcatWithSeparator},
	sql.NewFunction0("connection_id", sql.Uint32, connIDFuncLogic),
	NewUnaryFunc("cos", sql.Float64, CosFunc),
	NewUnaryFunc("cot", sql.Float64, CotFunc),
	sql.Function1{Name: "count", Fn: func(e sql.Expression) sql.Expression { return aggregation.NewCount(e) }},
	NewUnaryFunc("crc32", sql.Uint32, Crc32Func),
	sql.NewFunction0("curdate", sql.LongText, currDateLogic),
	sql.NewFunction0("current_date", sql.LongText, currDateLogic),
	sql.NewFunction0("current_time", sql.LongText, currTimeLogic),
	sql.NewFunction0("current_timestamp", sql.Datetime, currDatetimeLogic),
	sql.NewFunction0("current_user", sql.LongText, userFuncLogic),
	sql.NewFunction0("curtime", sql.LongText, currTimeLogic),
	sql.Function1{Name: "date", Fn: NewDate},
	sql.FunctionN{Name: "date_add", Fn: NewDateAdd},
	sql.Function2{Name: "date_format", Fn: NewDateFormat},
	sql.FunctionN{Name: "date_sub", Fn: NewDateSub},
	sql.FunctionN{Name: "datetime", Fn: NewDatetime},
	sql.Function1{Name: "day", Fn: NewDay},
	NewUnaryDatetimeFunc("dayname", sql.LongText, dayNameFuncLogic),
	sql.Function1{Name: "dayofmonth", Fn: NewDay},
	sql.Function1{Name: "dayofweek", Fn: NewDayOfWeek},
	sql.Function1{Name: "dayofyear", Fn: NewDayOfYear},
	NewUnaryFunc("degrees", sql.Float64, DegreesFunc),
	sql.Function1{Name: "explode", Fn: NewExplode},
	sql.Function1{Name: "first", Fn: func(e sql.Expression) sql.Expression { return aggregation.NewFirst(e) }},
	sql.Function1{Name: "floor", Fn: NewFloor},
	sql.Function1{Name: "from_base64", Fn: NewFromBase64},
	sql.FunctionN{Name: "greatest", Fn: NewGreatest},
	NewUnaryFunc("hex", sql.Text, HexFunc),
	sql.Function1{Name: "hour", Fn: NewHour},
	sql.Function3{Name: "if", Fn: NewIf},
	sql.Function2{Name: "ifnull", Fn: NewIfNull},
	sql.Function2{Name: "instr", Fn: NewInstr},
	sql.Function1{Name: "is_binary", Fn: NewIsBinary},
	sql.FunctionN{Name: "json_extract", Fn: NewJSONExtract},
	sql.Function1{Name: "json_unquote", Fn: NewJSONUnquote},
	sql.Function1{Name: "last", Fn: func(e sql.Expression) sql.Expression { return aggregation.NewLast(e) }},
	sql.Function1{Name: "lcase", Fn: NewLower},
	sql.FunctionN{Name: "least", Fn: NewLeast},
	sql.Function2{Name: "left", Fn: NewLeft},
	sql.Function1{Name: "length", Fn: NewLength},
	sql.Function1{Name: "ln", Fn: NewLogBaseFunc(float64(math.E))},
	sql.FunctionN{Name: "log", Fn: NewLog},
	sql.Function1{Name: "log10", Fn: NewLogBaseFunc(float64(10))},
	sql.Function1{Name: "log2", Fn: NewLogBaseFunc(float64(2))},
	sql.Function1{Name: "lower", Fn: NewLower},
	sql.FunctionN{Name: "lpad", Fn: NewPadFunc(lPadType)},
	sql.Function1{Name: "ltrim", Fn: NewTrimFunc(lTrimType)},
	sql.Function1{Name: "max", Fn: func(e sql.Expression) sql.Expression { return aggregation.NewMax(e) }},
	NewUnaryDatetimeFunc("microsecond", sql.Uint64, microsecondFuncLogic),
	sql.FunctionN{Name: "mid", Fn: NewSubstring},
	sql.Function1{Name: "min", Fn: func(e sql.Expression) sql.Expression { return aggregation.NewMin(e) }},
	sql.Function1{Name: "minute", Fn: NewMinute},
	sql.Function1{Name: "month", Fn: NewMonth},
	NewUnaryDatetimeFunc("monthname", sql.LongText, monthNameFuncLogic),
	sql.FunctionN{Name: "now", Fn: NewNow},
	sql.Function2{Name: "nullif", Fn: NewNullIf},
	sql.Function2{Name: "pow", Fn: NewPower},
	sql.Function2{Name: "power", Fn: NewPower},
	NewUnaryFunc("radians", sql.Float64, RadiansFunc),
	sql.FunctionN{Name: "rand", Fn: NewRand},
	sql.FunctionN{Name: "regexp_matches", Fn: NewRegexpMatches},
	sql.Function2{Name: "repeat", Fn: NewRepeat},
	sql.Function3{Name: "replace", Fn: NewReplace},
	sql.Function1{Name: "reverse", Fn: NewReverse},
	sql.FunctionN{Name: "round", Fn: NewRound},
	sql.FunctionN{Name: "rpad", Fn: NewPadFunc(rPadType)},
	sql.Function1{Name: "rtrim", Fn: NewTrimFunc(rTrimType)},
	sql.Function1{Name: "second", Fn: NewSecond},
	NewUnaryFunc("sign", sql.Int8, SignFunc),
	NewUnaryFunc("sin", sql.Float64, SinFunc),
	sql.Function1{Name: "sleep", Fn: NewSleep},
	sql.Function1{Name: "soundex", Fn: NewSoundex},
	sql.Function2{Name: "split", Fn: NewSplit},
	sql.Function1{Name: "sqrt", Fn: NewSqrt},
	sql.FunctionN{Name: "substr", Fn: NewSubstring},
	sql.FunctionN{Name: "substring", Fn: NewSubstring},
	sql.Function3{Name: "substring_index", Fn: NewSubstringIndex},
	sql.Function1{Name: "sum", Fn: func(e sql.Expression) sql.Expression { return aggregation.NewSum(e) }},
	NewUnaryFunc("tan", sql.Float64, TanFunc),
	NewUnaryDatetimeFunc("time_to_sec", sql.Uint64, timeToSecFuncLogic),
	sql.FunctionN{Name: "timestamp", Fn: NewTimestamp},
	sql.Function1{Name: "to_base64", Fn: NewToBase64},
	sql.Function1{Name: "trim", Fn: NewTrimFunc(bTrimType)},
	sql.Function1{Name: "ucase", Fn: NewUpper},
	NewUnaryFunc("unhex", sql.Text, UnhexFunc),
	sql.FunctionN{Name: "unix_timestamp", Fn: NewUnixTimestamp},
	sql.Function1{Name: "upper", Fn: NewUpper},
	sql.NewFunction0("user", sql.LongText, userFuncLogic),
	sql.FunctionN{Name: "week", Fn: NewWeek},
	sql.Function1{Name: "weekday", Fn: NewWeekday},
	NewUnaryDatetimeFunc("weekofyear", sql.Uint64, weekFuncLogic),
	sql.Function1{Name: "year", Fn: NewYear},
	sql.FunctionN{Name: "yearweek", Fn: NewYearWeek},
}

func GetLockingFuncs(ls *sql.LockSubsystem) []sql.Function {
	return []sql.Function{
		sql.Function2{Name: "get_lock", Fn: CreateNewGetLock(ls)},
		NewNamedLockFunc(ls, "is_free_lock", sql.Int8, IsFreeLockFunc),
		NewNamedLockFunc(ls, "is_used_lock", sql.Uint32, IsUsedLockFunc),
		sql.NewFunction0("release_all_locks", sql.Int32, ReleaseAllLocksForLS(ls)),
		NewNamedLockFunc(ls, "release_lock", sql.Int8, ReleaseLockFunc),
	}
}
