namespace py calculator

typedef i32 int

struct ResultResponse {
    1: optional int result,
}

service CalculatorService {
    ResultResponse calculate(
        1: required string op,
        2: required int a,
        3: required int b,
    )
}