namespace py com.oyl

typedef i32 int

struct Work {
    1: optional int result,
}

service OylWorkService {
    Work work(
        1: required string op,
        2: required int a,
        3: required int b,
    )
}