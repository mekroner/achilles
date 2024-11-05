use std::{collections::HashMap, fmt, ops::AddAssign};

use crate::{
    eval::{check_results::read_test_set_results_from_file, evaluator::ResultRelation},
    test_case_exec::{read_test_set_execs_from_file, TestCaseExecStatus},
    test_case_gen::oracle::QueryGenStrategy,
    LancerConfig,
};

#[derive(Default, Clone)]
struct SummaryStatsEntry {
    // Exec
    total_count: u32,
    success_count: u32,
    fail_count: u32,
    timeout_count: u32,
    skipped_count: u32,
    // Eval
    total_res_count: u32,
    equal_count: u32,
    reorder_count: u32,
    diff_count: u32,
}

impl SummaryStatsEntry {
    fn success_rates(&self) -> SummerySuccessRate {
        let exec_success_rate = self.success_count as f32 / self.total_count as f32;
        let eval_success_rate =
            (self.equal_count + self.reorder_count) as f32 / self.total_res_count as f32;
        SummerySuccessRate {
            exec_success_rate,
            eval_success_rate,
        }
    }
}

impl AddAssign for SummaryStatsEntry {
    fn add_assign(&mut self, rhs: Self) {
        self.total_count += rhs.total_count;
        self.success_count += rhs.success_count;
        self.fail_count += rhs.fail_count;
        self.timeout_count += rhs.timeout_count;
        self.skipped_count += rhs.skipped_count;
        self.total_res_count += rhs.total_res_count;
        self.equal_count += rhs.equal_count;
        self.reorder_count += rhs.reorder_count;
        self.diff_count += rhs.diff_count;
    }
}

struct SummerySuccessRate {
    exec_success_rate: f32,
    eval_success_rate: f32,
}

struct SummerySuccessRateVariance {
    exec_variance: f32,
    eval_variance: f32,
}

#[derive(Default)]
struct SummaryStats {
    stats: HashMap<QueryGenStrategy, SummaryStatsEntry>,
}

impl SummaryStats {
    fn success_rates(&self) -> HashMap<QueryGenStrategy, SummerySuccessRate> {
        let mut map = HashMap::new();
        for (strategy, entry) in &self.stats {
            map.insert(*strategy, entry.success_rates());
        }
        map
    }

    fn total_success_rates(&self) -> SummerySuccessRate {
        let mut all_totals = SummaryStatsEntry::default();
        for (_, entry) in &self.stats {
            all_totals += entry.clone();
        }
        all_totals.success_rates()
    }
}

impl AddAssign for SummaryStats {
    fn add_assign(&mut self, rhs: Self) {
        for (strategy, entry) in rhs.stats {
            self.stats
                .entry(strategy)
                .and_modify(|x| *x += entry.clone())
                .or_insert(entry.clone());
        }
    }
}

impl fmt::Display for SummaryStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // header
        writeln!(
            f,
            "{:<15} | {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} | {:<10} {:<10} {:<10} {:<10} {:<10}",
            "Oracle",
            "Total",
            "Success",
            "Succ Rate",
            "Fail",
            "Timeout",
            "Skipped",
            "TotalRes",
            "Succ Rate",
            "Equal",
            "Reorder",
            "Diff"
        )?;
        writeln!(f, "{}", "-".repeat(111))?;

        let mut all_totals = SummaryStatsEntry::default();

        for (strategy, entry) in &self.stats {
            writeln!(
                f,
                "{:<15} | {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} | {:<10} {:<10} {:<10} {:<10} {:<10}",
                format!("{:?}", strategy),
                entry.total_count,
                entry.success_count,
                entry.success_count as f32 / entry.total_count as f32,
                entry.fail_count,
                entry.timeout_count,
                entry.skipped_count,
                entry.total_res_count,
                (entry.equal_count + entry.reorder_count) as f32 / entry.total_res_count as f32,
                entry.equal_count,
                entry.reorder_count,
                entry.diff_count
            )?;

            // Accumulate totals for the "all" row
            all_totals.total_count += entry.total_count;
            all_totals.success_count += entry.success_count;
            all_totals.fail_count += entry.fail_count;
            all_totals.timeout_count += entry.timeout_count;
            all_totals.skipped_count += entry.skipped_count;
            all_totals.total_res_count += entry.total_res_count;
            all_totals.equal_count += entry.equal_count;
            all_totals.reorder_count += entry.reorder_count;
            all_totals.diff_count += entry.diff_count;
        }

        writeln!(
            f,
            "{:<15} | {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} | {:<10} {:<10} {:<10} {:<10} {:<10}",
            "All",
            all_totals.total_count,
            all_totals.success_count,
            all_totals.success_count as f32 / all_totals.total_count as f32,
            all_totals.fail_count,
            all_totals.timeout_count,
            all_totals.skipped_count,
            all_totals.total_res_count,
            (all_totals.equal_count + all_totals.reorder_count) as f32 / all_totals.total_res_count as f32,
            all_totals.equal_count,
            all_totals.reorder_count,
            all_totals.diff_count
        )?;

        Ok(())
    }
}

pub fn summary_operation(config: &LancerConfig) {
    log::info!("Starting Summary Mode.");
    let mut total_stats = SummaryStats::default();
    for run_id in 0..config.test_config.test_run_count {
        println!("---( RUN {run_id} ) ---");
        let stats = calc_summery(run_id, config);
        println!("{stats}");
        total_stats += stats;
    }
    println!("---( TOTAL ) ---");
    println!("{total_stats}");
}

fn calc_summery(run_id: u32, config: &LancerConfig) -> SummaryStats {
    let test_set_execs = read_test_set_execs_from_file(run_id, &config);
    let test_set_results = read_test_set_results_from_file(run_id, &config);

    let mut sum_stats = SummaryStats::default();
    for test_set_exec in test_set_execs {
        let iter = std::iter::once(&test_set_exec.origin).chain(test_set_exec.others.iter());
        let strat = test_set_exec.strategy;
        let stats = sum_stats.stats.entry(strat).or_default();

        for exec in iter {
            stats.total_count += 1;
            match exec.status {
                TestCaseExecStatus::Success => stats.success_count += 1,
                TestCaseExecStatus::Failed(_) => stats.fail_count += 1,
                TestCaseExecStatus::TimedOut => stats.timeout_count += 1,
                TestCaseExecStatus::Skipped => stats.skipped_count += 1,
            }
        }
    }

    for test_set_result in test_set_results {
        let strat = test_set_result.strategy;
        let stats = sum_stats.stats.entry(strat).or_default();
        for res in &test_set_result.test_cases {
            stats.total_res_count += 1;
            match res.relation {
                ResultRelation::Equal => stats.equal_count += 1,
                ResultRelation::Reordered => stats.reorder_count += 1,
                ResultRelation::Diff => stats.diff_count += 1,
            }
        }
    }
    sum_stats
}
