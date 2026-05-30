#!/bin/bash
# L7 DAO Mesh (:9510)
cd /home/agent/data/sites/relay-mesh && python3 -c "
import dao_mesh
dao_mesh.PORT = 9510
dao_mesh.PIDFILE = '/tmp/snin_dao.pid'
dao_mesh.run_server()
" >> logs/dao.log 2>&1 &
