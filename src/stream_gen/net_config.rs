use std::net::Ipv4Addr;

pub struct NetworkConfig {
    pub coord_ip: Ipv4Addr,
    pub rest_port: u16,
    pub rpc_port: u16,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            coord_ip: Ipv4Addr::LOCALHOST,
            rest_port: 8000,
            rpc_port: 4000,
        }
    }
}
