use std::path::Path;

use achilles::eval::evaluator::*;

macro_rules! eval_test {
    ($($name:ident: ($other_path:expr, $expected:expr),)*) => {
        $(
        #[test]
        fn $name() {
            let default_path = Path::new("./tests/eval/assets/default.csv");
            let other_path = Path::new($other_path);
            let res = compare_files(default_path, other_path).unwrap();
            assert_eq!($expected, res);
        })*
    };
}

eval_test!{
    equal_test: ("./tests/eval/assets/default.csv", ResultRelation::Equal),
    reordered_test: ("./tests/eval/assets/default_reordered.csv", ResultRelation::Reordered),
    empty_test: ("./tests/eval/assets/empty.csv", ResultRelation::Diff),
    lines_missing_test: ("./tests/eval/assets/default_lines_missing.csv", ResultRelation::Diff),
    lines_double_test: ("./tests/eval/assets/default_lines_double.csv", ResultRelation::Diff),
    missing_and_double_test: ("./tests/eval/assets/default_missing_and_double.csv", ResultRelation::Diff),
}
