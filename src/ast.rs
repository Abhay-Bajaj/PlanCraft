use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Query {
    pub from: String,
    pub select: Vec<String>,

    #[serde(default)]
    pub r#where: Vec<Predicate>,

    #[serde(default)]
    pub group_by: Vec<String>,

    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Predicate {
    pub col: String,
    pub op: String,
    pub val: serde_json::Value,
}
