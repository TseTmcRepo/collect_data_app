
#import constant_database_data
from my_database_info import vps1_local_access, server_remote_access_from_vps, get_database_info

# vps1
client_id = 10
local_db_info = get_database_info(vps1_local_access, 'bourse_tsetmc_and_analyze_data')
# local_db_info = constant_database_data.vps1_tsetmc_raw_data_info_local_access

server_db_info = local_db_info
# server_db_info = constant_database_data.vps1_tsetmc_raw_data_info_local_access

# laptop
#client_id = 2
#local_db_info = constant_database_data.laptop_client_role_db_info
#server_db_info = constant_database_data.pc1_server_role_db_info

# pc1
#client_id = 1
#local_db_info = constant_database_data.pc1_client_role_db_info_local
#server_db_info = constant_database_data.pc1_server_role_db_info

# server
#client_id = 3
#local_db_info = constant_database_data.server_client_role_db_info
#server_db_info = constant_database_data.pc1_server_role_db_info

# pc_home
#client_id = 4
#local_db_info = constant_database_data.pc_home_client_role_db_info
#server_db_info = constant_database_data.pc1_server_role_db_info
