use uuid::Uuid;

pub struct ClusterConfig {
    pub port: u16,
    pub mesh_port: u16,
    pub host: String,
    pub mesh_bind_host: String,
    pub mesh_advertise_host: String,
    pub data_dir: String,
    pub node_id: Option<Uuid>,
    pub dns_query: Option<String>,
}

impl ClusterConfig {
    pub fn from_env() -> Self {
        let port: u16 = std::env::var("PORT")
            .unwrap_or_else(|_| "3000".to_string())
            .parse()
            .expect("PORT must be a number");

        let mesh_port: u16 = std::env::var("MESH_PORT")
            .unwrap_or_else(|_| "3001".to_string())
            .parse()
            .expect("MESH_PORT must be a number");

        let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
        let mesh_bind_host = std::env::var("MESH_BIND_HOST").unwrap_or_else(|_| host.clone());
        let mesh_advertise_host = std::env::var("MESH_ADVERTISE_HOST")
            .ok()
            .or_else(|| std::env::var("POD_IP").ok())
            .unwrap_or_else(|| mesh_bind_host.clone());

        let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| "./data".to_string());

        let node_id = std::env::var("NODE_ID")
            .ok()
            .and_then(|s| Uuid::parse_str(&s).ok());

        let dns_query = std::env::var("DNS_QUERY").ok();

        ClusterConfig {
            port,
            mesh_port,
            host,
            mesh_bind_host,
            mesh_advertise_host,
            data_dir,
            node_id,
            dns_query,
        }
    }

    /// Override select fields on a config produced from `from_env()`.
    pub fn with_overrides(
        mut self,
        mesh_port: Option<u16>,
        mesh_advertise_host: Option<String>,
        data_dir: Option<String>,
    ) -> Self {
        if let Some(p) = mesh_port {
            self.mesh_port = p;
        }
        if let Some(h) = mesh_advertise_host {
            self.mesh_advertise_host = h;
        }
        if let Some(d) = data_dir {
            self.data_dir = d;
        }
        self
    }
}
