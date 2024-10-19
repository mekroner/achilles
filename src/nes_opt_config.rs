use yaml_rust2::yaml::Hash;
use yaml_rust2::Yaml;

#[derive(Debug, Clone)]
pub enum MemoryLayoutPolicy {
    ForceRowLayout,
    ForceColumnLayout,
}

#[derive(Debug, Clone)]
pub enum QueryMergerRule {
    DefaultQueryMergerRule,
    Z3SignatureBasedCompleteQueryMergerRule,
    Z3SignatureBasedPartialQueryMergerRule,
    HashSignatureBasedCompleteQueryMergerRule,
    HashSignatureBasedPartialQueryMergerRule,
    HybridCompleteQueryMergerRule,
}

#[derive(Debug, Clone)]
pub enum JoinOptMode {
    None,
    Matrix,
    Nemo,
}

#[derive(Debug, Clone)]
pub enum PlacementAmendmentMode {
    Pessimistic,
    Optimistic,
}

#[derive(Debug, Clone)]
pub struct NesOptConfig {
    pub join_optimization_mode: JoinOptMode,
    pub enable_incremental_placement: bool,
    pub placement_amendment_thread_count: u32,
    pub placement_amendment_mode: PlacementAmendmentMode,
    pub enable_nemo_placement: bool,
    pub perform_only_source_operator_expansion: bool,
    pub perform_advance_semantic_validation: bool,
    pub memory_layout_policy: MemoryLayoutPolicy,
    pub allow_exhaustive_containment_check: bool,
    pub query_merger_rule: QueryMergerRule,
}

impl Default for NesOptConfig {
    fn default() -> Self {
        Self {
            join_optimization_mode: JoinOptMode::None,
            enable_incremental_placement: false,
            placement_amendment_thread_count: 1,
            placement_amendment_mode: PlacementAmendmentMode::Pessimistic,
            enable_nemo_placement: false,
            perform_only_source_operator_expansion: false,
            perform_advance_semantic_validation: false,
            memory_layout_policy: MemoryLayoutPolicy::ForceRowLayout,
            allow_exhaustive_containment_check: false,
            query_merger_rule: QueryMergerRule::DefaultQueryMergerRule,
        }
    }
}

impl Into<Yaml> for &MemoryLayoutPolicy {
    fn into(self) -> Yaml {
        match self {
            MemoryLayoutPolicy::ForceRowLayout => Yaml::String("FORCE_ROW_LAYOUT".to_string()),
            MemoryLayoutPolicy::ForceColumnLayout => {
                Yaml::String("FORCE_COLUMN_LAYOUT".to_string())
            }
        }
    }
}

impl TryFrom<&Yaml> for MemoryLayoutPolicy {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let Some(policy) = value.as_str() else {
            return Err("Invalid memory layout policy".to_string());
        };
        match policy {
            "FORCE_ROW_LAYOUT" => Ok(MemoryLayoutPolicy::ForceRowLayout),
            "FORCE_COLUMN_LAYOUT" => Ok(MemoryLayoutPolicy::ForceColumnLayout),
            _ => Err("Invalid memory layout policy".to_string()),
        }
    }
}

impl Into<Yaml> for &QueryMergerRule {
    fn into(self) -> Yaml {
        match self {
            QueryMergerRule::DefaultQueryMergerRule => {
                Yaml::String("DefaultQueryMergerRule".to_string())
            }
            QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule => {
                Yaml::String("Z3SignatureBasedCompleteQueryMergerRule".to_string())
            }
            QueryMergerRule::Z3SignatureBasedPartialQueryMergerRule => {
                Yaml::String("Z3SignatureBasedPartialQueryMergerRule".to_string())
            }
            QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule => {
                Yaml::String("HashSignatureBasedCompleteQueryMergerRule".to_string())
            }
            QueryMergerRule::HashSignatureBasedPartialQueryMergerRule => {
                Yaml::String("HashSignatureBasedPartialQueryMergerRule".to_string())
            }
            QueryMergerRule::HybridCompleteQueryMergerRule => {
                Yaml::String("HybridCompleteQueryMergerRule".to_string())
            }
        }
    }
}

impl TryFrom<&Yaml> for QueryMergerRule {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let Some(rule) = value.as_str() else {
            return Err("Invalid query merger rule".to_string());
        };
        match rule {
            "DefaultQueryMergerRule" => Ok(QueryMergerRule::DefaultQueryMergerRule),
            "Z3SignatureBasedCompleteQueryMergerRule" => {
                Ok(QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule)
            }
            "Z3SignatureBasedPartialQueryMergerRule" => {
                Ok(QueryMergerRule::Z3SignatureBasedPartialQueryMergerRule)
            }
            "HashSignatureBasedCompleteQueryMergerRule" => {
                Ok(QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule)
            }
            "HashSignatureBasedPartialQueryMergerRule" => {
                Ok(QueryMergerRule::HashSignatureBasedPartialQueryMergerRule)
            }
            "HybridCompleteQueryMergerRule" => Ok(QueryMergerRule::HybridCompleteQueryMergerRule),
            _ => Err("Invalid query merger rule".to_string()),
        }
    }
}

impl Into<Yaml> for &JoinOptMode {
    fn into(self) -> Yaml {
        match self {
            JoinOptMode::None => Yaml::String("NONE".to_string()),
            JoinOptMode::Matrix => Yaml::String("MATRIX".to_string()),
            JoinOptMode::Nemo => Yaml::String("NEMO".to_string()),
        }
    }
}

impl TryFrom<&Yaml> for JoinOptMode {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        match value.as_str() {
            Some("NONE") => Ok(JoinOptMode::None),
            Some("MATRIX") => Ok(JoinOptMode::Matrix),
            Some("NEMO") => Ok(JoinOptMode::Nemo),
            _ => Err("Failed to parse join_optimization_mode.".to_string()),
        }
    }
}

impl Into<Yaml> for &PlacementAmendmentMode {
    fn into(self) -> Yaml {
        match self {
            PlacementAmendmentMode::Pessimistic => Yaml::String("PESSIMISTIC".to_string()),
            PlacementAmendmentMode::Optimistic => Yaml::String("OPTIMISTIC".to_string()),
        }
    }
}

impl TryFrom<&Yaml> for PlacementAmendmentMode {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        match value.as_str() {
            Some("PESSIMISTIC") => Ok(PlacementAmendmentMode::Pessimistic),
            Some("OPTIMISTIC") => Ok(PlacementAmendmentMode::Optimistic),
            _ => Err("Failed to parse placement_amendment_mode.".to_string()),
        }
    }
}

impl Into<Yaml> for &NesOptConfig {
    fn into(self) -> Yaml {
        let mut config_map: Hash = Hash::new();
        config_map.insert(
            Yaml::String("distributedJoinOptimizationMode".to_string()),
            (&self.join_optimization_mode).into(),
        );
        config_map.insert(
            Yaml::String("enableIncrementalPlacement".to_string()),
            Yaml::Boolean(self.enable_incremental_placement),
        );
        config_map.insert(
            Yaml::String("placementAmendmentThreadCount".to_string()),
            Yaml::Integer(self.placement_amendment_thread_count as i64),
        );
        config_map.insert(
            Yaml::String("placementAmendmentMode".to_string()),
            (&self.placement_amendment_mode).into(),
        );
        config_map.insert(
            Yaml::String("enableNemoPlacement".to_string()),
            Yaml::Boolean(self.enable_nemo_placement),
        );
        config_map.insert(
            Yaml::String("performOnlySourceOperatorExpansion".to_string()),
            Yaml::Boolean(self.perform_only_source_operator_expansion),
        );
        config_map.insert(
            Yaml::String("advanceSemanticValidation".to_string()),
            Yaml::Boolean(self.perform_advance_semantic_validation),
        );
        config_map.insert(
            Yaml::String("memoryLayoutPolicy".to_string()),
            (&self.memory_layout_policy).into(),
        );
        config_map.insert(
            Yaml::String("allowExhaustiveContainmentCheck".to_string()),
            Yaml::Boolean(self.allow_exhaustive_containment_check),
        );
        config_map.insert(
            Yaml::String("queryMergerRule".to_string()),
            (&self.query_merger_rule).into(),
        );
        Yaml::Hash(config_map)
    }
}

impl TryFrom<&Yaml> for NesOptConfig {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let join_optimization_mode = JoinOptMode::try_from(&value["distributedJoinOptimizationMode"])?;

        let Yaml::Boolean(enable_incremental_placement) = value["enableIncrementalPlacement"]
        else {
            return Err("Failed to parse enableIncrementalPlacement".to_string());
        };

        let Yaml::Integer(placement_amendment_thread_count) =
            value["placementAmendmentThreadCount"]
        else {
            return Err("Failed to parse placementAmendmentThreadCount".to_string());
        };

        let placement_amendment_mode =
            PlacementAmendmentMode::try_from(&value["placementAmendmentMode"])?;

        let Yaml::Boolean(enable_nemo_placement) = value["enableNemoPlacement"] else {
            return Err("Failed to parse enableNemoPlacement".to_string());
        };

        let Yaml::Boolean(perform_only_source_operator_expansion) = value["performOnlySourceOperatorExpansion"]
        else {
            return Err("Failed to parse performOnlySourceOperatorExpansion".to_string());
        };

        let Yaml::Boolean(perform_advance_semantic_validation) =
            value["advanceSemanticValidation"]
        else {
            return Err("Failed to parse performAdvanceSemanticValidation".to_string());
        };

        let memory_layout_policy = MemoryLayoutPolicy::try_from(&value["memoryLayoutPolicy"])?;

        let Yaml::Boolean(allow_exhaustive_containment_check) =
            value["allowExhaustiveContainmentCheck"]
        else {
            return Err("Failed to parse allowExhaustiveContainmentCheck".to_string());
        };

        let query_merger_rule = QueryMergerRule::try_from(&value["queryMergerRule"])?;

        Ok(NesOptConfig {
            join_optimization_mode,
            enable_incremental_placement,
            placement_amendment_thread_count: placement_amendment_thread_count as u32,
            placement_amendment_mode,
            enable_nemo_placement,
            perform_only_source_operator_expansion,
            perform_advance_semantic_validation,
            memory_layout_policy,
            allow_exhaustive_containment_check,
            query_merger_rule,
        })
    }
}
