use yaml_rust2::yaml::Hash;
use yaml_rust2::Yaml;

#[derive(Debug, Clone)]
pub enum PipeliningStrategy {
    OperatorFusion,
    OperatorAtATime,
}

#[derive(Debug, Clone)]
pub enum CompilationStrategy {
    Fast,
    Debug,
    Optimize,
}

#[derive(Debug, Clone)]
pub enum OutputBufferOptimizationLevel {
    All,
    No,
    OnlyInplaceOperationsNoFallback,
    ReuseInputBufferAndOmitOverflowCheckNoFallback,
    ReuseInputBufferNoFallback,
    OmitOverflowCheckNoFallback,
}

#[derive(Debug, Clone)]
pub enum WindowingStrategy {
    Legacy,
    Slicing,
    Bucketing,
}

#[derive(Debug, Clone)]
pub enum QueryCompilerType {
    DefaultQueryCompiler,
    NautilusQueryCompiler,
}

#[derive(Debug, Clone)]
pub struct NesQueryCompilerConfig {
    pub pipelining_strategy: PipeliningStrategy,
    pub compilation_strategy: CompilationStrategy,
    pub output_buffer_optimization_level: OutputBufferOptimizationLevel,
    pub windowing_strategy: WindowingStrategy,
    pub query_compiler_type: QueryCompilerType,
}

impl Default for NesQueryCompilerConfig {
    fn default() -> Self {
        Self {
            pipelining_strategy: PipeliningStrategy::OperatorFusion,
            compilation_strategy: CompilationStrategy::Optimize,
            output_buffer_optimization_level: OutputBufferOptimizationLevel::All,
            windowing_strategy: WindowingStrategy::Legacy,
            query_compiler_type: QueryCompilerType::DefaultQueryCompiler,
        }
    }
}

impl Into<Yaml> for &PipeliningStrategy {
    fn into(self) -> Yaml {
        match self {
            PipeliningStrategy::OperatorFusion => Yaml::String("OPERATOR_FUSION".to_string()),
            PipeliningStrategy::OperatorAtATime => Yaml::String("OPERATOR_AT_A_TIME".to_string()),
        }
    }
}

impl TryFrom<&Yaml> for PipeliningStrategy {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let Some(str) = value.as_str() else {
            return Err(format!("Unable to parse Pipelining Strategy."));
        };
        match str {
            "OPERATOR_FUSION" => Ok(PipeliningStrategy::OperatorFusion),
            "OPERATOR_AT_A_TIME" => Ok(PipeliningStrategy::OperatorAtATime),
            err => Err(format!("Unknown Pipelining Strategy: {err}")),
        }
    }
}

impl Into<Yaml> for &CompilationStrategy {
    fn into(self) -> Yaml {
        match self {
            CompilationStrategy::Fast => Yaml::String("FAST".to_string()),
            CompilationStrategy::Debug => Yaml::String("DEBUG".to_string()),
            CompilationStrategy::Optimize => Yaml::String("OPTIMIZE".to_string()),
        }
    }
}

impl TryFrom<&Yaml> for CompilationStrategy {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let Some(str) = value.as_str() else {
            return Err(format!("Unable to parse Compilation Strategy."));
        };
        match str {
            "FAST" => Ok(CompilationStrategy::Fast),
            "DEBUG" => Ok(CompilationStrategy::Debug),
            "OPTIMIZE" => Ok(CompilationStrategy::Optimize),
            err => Err(format!("Unknown Compilation Strategy: {err}")),
        }
    }
}

impl Into<Yaml> for &OutputBufferOptimizationLevel {
    fn into(self) -> Yaml {
        match self {
            OutputBufferOptimizationLevel::All => Yaml::String("ALL".to_string()),
            OutputBufferOptimizationLevel::No => Yaml::String("NO".to_string()),
            OutputBufferOptimizationLevel::OnlyInplaceOperationsNoFallback => {
                Yaml::String("ONLY_INPLACE_OPERATIONS_NO_FALLBACK".to_string())
            }
            OutputBufferOptimizationLevel::ReuseInputBufferAndOmitOverflowCheckNoFallback => {
                Yaml::String("REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK_NO_FALLBACK".to_string())
            }
            OutputBufferOptimizationLevel::ReuseInputBufferNoFallback => {
                Yaml::String("REUSE_INPUT_BUFFER_NO_FALLBACK".to_string())
            }
            OutputBufferOptimizationLevel::OmitOverflowCheckNoFallback => {
                Yaml::String("OMIT_OVERFLOW_CHECK_NO_FALLBACK".to_string())
            }
        }
    }
}

impl TryFrom<&Yaml> for OutputBufferOptimizationLevel {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let Some(str) = value.as_str() else {
            return Err(format!("Unable to parse OutputBufferOptimizationLevel."));
        };
        match str {
            "ALL" => Ok(OutputBufferOptimizationLevel::All),
            "NO" => Ok(OutputBufferOptimizationLevel::No),
            "ONLY_INPLACE_OPERATIONS_NO_FALLBACK" => Ok(OutputBufferOptimizationLevel::OnlyInplaceOperationsNoFallback),
            "REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK_NO_FALLBACK" => Ok(OutputBufferOptimizationLevel::ReuseInputBufferAndOmitOverflowCheckNoFallback),
            "REUSE_INPUT_BUFFER_NO_FALLBACK" => Ok(OutputBufferOptimizationLevel::ReuseInputBufferNoFallback),
            "OMIT_OVERFLOW_CHECK_NO_FALLBACK" => Ok(OutputBufferOptimizationLevel::OmitOverflowCheckNoFallback),
            err => Err(format!("Unknown OutputBufferOptimizationLevel: {err}")),
        }
    }
}

impl Into<Yaml> for &WindowingStrategy {
    fn into(self) -> Yaml {
        match self {
            WindowingStrategy::Legacy => Yaml::String("LEGACY".to_string()),
            WindowingStrategy::Slicing => Yaml::String("SLICING".to_string()),
            WindowingStrategy::Bucketing => Yaml::String("BUCKETING".to_string()),
        }
    }
}

impl TryFrom<&Yaml> for WindowingStrategy {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let Some(str) = value.as_str() else {
            return Err(format!("Unable to parse WindowingStrategy."));
        };
        match str {
            "LEGACY" => Ok(WindowingStrategy::Legacy),
            "SLICING" => Ok(WindowingStrategy::Slicing),
            "BUCKETING" => Ok(WindowingStrategy::Bucketing),
            err => Err(format!("Unknown WindowingStrategy: {err}")),
        }
    }
}

impl Into<Yaml> for &QueryCompilerType {
    fn into(self) -> Yaml {
        match self {
            QueryCompilerType::DefaultQueryCompiler => {
                Yaml::String("DEFAULT_QUERY_COMPILER".to_string())
            }
            QueryCompilerType::NautilusQueryCompiler => {
                Yaml::String("NAUTILUS_QUERY_COMPILER".to_string())
            }
        }
    }
}

impl TryFrom<&Yaml> for QueryCompilerType {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let Some(str) = value.as_str() else {
            return Err(format!("Unable to parse DefaultQueryCompiler."));
        };
        match str {
            "DEFAULT_QUERY_COMPILER" => Ok(QueryCompilerType::DefaultQueryCompiler),
            "NAUTILUS_QUERY_COMPILER" => Ok(QueryCompilerType::NautilusQueryCompiler),
            err => Err(format!("Unknown QueryCompilerType: {err}")),
        }
    }
}

impl Into<Yaml> for &NesQueryCompilerConfig {
    fn into(self) -> Yaml {
        let mut config_map: Hash = Hash::new();

        config_map.insert(
            Yaml::String("pipeliningStrategy".to_string()),
            (&self.pipelining_strategy).into(),
        );

        config_map.insert(
            Yaml::String("compilationStrategy".to_string()),
            (&self.compilation_strategy).into(),
        );

        config_map.insert(
            Yaml::String("outputBufferOptimizationLevel".to_string()),
            (&self.output_buffer_optimization_level).into(),
        );

        config_map.insert(
            Yaml::String("windowingStrategy".to_string()),
            (&self.windowing_strategy).into(),
        );

        config_map.insert(
            Yaml::String("queryCompilerType".to_string()),
            (&self.query_compiler_type).into(),
        );

        Yaml::Hash(config_map)
    }
}
