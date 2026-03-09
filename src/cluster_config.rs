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
    pub worker_threads: usize,
    pub max_concurrent_tasks: usize,
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

        let node_id = std::env::var("NODE_ID").ok().and_then(|s| {
            if let Ok(u) = Uuid::parse_str(&s) {
                Some(u)
            } else {
                // Deterministic UUID from non-uuid string (e.g. pod name)
                Some(Uuid::new_v5(&Uuid::NAMESPACE_DNS, s.as_bytes()))
            }
        });

        let dns_query = std::env::var("DNS_QUERY").ok();

        let worker_threads = std::env::var("WORKER_THREADS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2); // Raspberry Pi preference: fewer cores often perform better under K8s limits

        let max_concurrent_tasks = std::env::var("MAX_CONCURRENT_TASKS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| {
                if cfg!(target_arch = "aarch64") {
                    50
                } else {
                    1000 // Default for more powerful architectures
                }
            });

        ClusterConfig {
            port,
            mesh_port,
            host,
            mesh_bind_host,
            mesh_advertise_host,
            data_dir,
            node_id,
            dns_query,
            worker_threads,
            max_concurrent_tasks,
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
