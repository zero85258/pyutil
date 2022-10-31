import os
import sys


class ETL:
    connections = {
        "sit": {
            "admin": {
                "OS_HOST": "10.142.254.195",
                "DB_HOST": "Mysql-01.c.c4-tw-ec-app-sit.internal",
                "DB_PORT": "3306",
                "DB_USERNAME": "root",
                "DB_PASSWORD": "This123ci#",
            },
            "app": {
                "OS_HOST": "10.142.254.195",
                "DB_HOST": "Mysql-01.c.c4-tw-ec-app-sit.internal",
                "DB_PORT": "3306",
                "DB_USERNAME": "root",
                "DB_PASSWORD": "This123ci#",
            },
            "red": {
                "OS_HOST": "10.142.254.195",
                "DB_HOST": "Mysql-01.c.c4-tw-ec-app-sit.internal",
                "DB_PORT": "3306",
                "DB_USERNAME": "root",
                "DB_PASSWORD": "This123ci#",
            }
        },
        "uat": {
            "admin": {
                "OS_HOST": "10.142.253.32",
                "DB_HOST": "mysql-admin-1.c.c4-tw-ec-app-test.internal",
                "DB_PORT": "3306",
                "DB_USERNAME": "root",
                "DB_PASSWORD": "This123ci#",
            },
            "app": {
                "OS_HOST": "10.142.253.27",
                "DB_HOST": "mysql-app-1.c.c4-tw-ec-app-test.internal",
                "DB_PORT": "3306",
                "DB_USERNAME": "root",
                "DB_PASSWORD": "This123ci#",
            },
            "red": {
                "OS_HOST": "10.142.253.29",
                "DB_HOST": "mysql-redenvelop-1.c.c4-tw-ec-app-test.internal",
                "DB_PORT": "3306",
                "DB_USERNAME": "root",
                "DB_PASSWORD": "This123ci#",
            }
        },
        "prod": {
            "admin": {
                "OS_HOST": "10.142.250.16",
                "DB_HOST": "mysql-admin-1.c.c4-tw-ec-app-prd.internal",
                "DB_PORT": "3306",
                "DB_USERNAME": "root",
                "DB_PASSWORD": "This123ci#",
            },
            "app": {
                "OS_HOST": "10.142.250.18",
                "DB_HOST": "mysql-app-1.c.c4-tw-ec-app-prd.internal",
                "DB_PORT": "3306",
                "DB_USERNAME": "root",
                "DB_PASSWORD": "This123ci#",
            },
            "red": {
                "OS_HOST": "10.142.250.15",
                "DB_HOST": "mysql-redenvelop-1.c.c4-tw-ec-app-prd.internal",
                "DB_PORT": "3306",
                "DB_USERNAME": "root",
                "DB_PASSWORD": "This123ci#",
            }
        },
        "dr": {
            "admin": {
                "OS_HOST": "10.142.255.131",
                "DB_HOST": "10.142.255.131",
                "DB_PORT": "3306",
                "DB_USERNAME": "root",
                "DB_PASSWORD": "This123ci#",
            },
            "app": {
                "OS_HOST": "10.142.255.132",
                "DB_HOST": "10.142.255.132",
                "DB_PORT": "3306",
                "DB_USERNAME": "root",
                "DB_PASSWORD": "This123ci#",
            },
            "red": {
                "OS_HOST": "10.142.255.133",
                "DB_HOST": "10.142.255.133",
                "DB_PORT": "3306",
                "DB_USERNAME": "root",
                "DB_PASSWORD": "This123ci#",
            }
        },
        "local1": {
            "admin": {
                "OS_HOST": "127.0.0.1",
                "DB_HOST": "127.0.0.1",
                "DB_PORT": "3307",
                "DB_USERNAME": "root",
                "DB_PASSWORD": "root",
            },
            "app": {
                "OS_HOST": "127.0.0.1",
                "DB_HOST": "127.0.0.1",
                "DB_PORT": "3308",
                "DB_USERNAME": "root",
                "DB_PASSWORD": "root",
            },
            "red": {
                "OS_HOST": "127.0.0.1",
                "DB_HOST": "127.0.0.1",
                "DB_PORT": "3309",
                "DB_USERNAME": "root",
                "DB_PASSWORD": "root",
            }
        },
        "local2": {
            "admin": {
                "OS_HOST": "127.0.0.1",
                "DB_HOST": "127.0.0.1",
                "DB_PORT": "3310",
                "DB_USERNAME": "root",
                "DB_PASSWORD": "root",
            },
            "app": {
                "OS_HOST": "127.0.0.1",
                "DB_HOST": "127.0.0.1",
                "DB_PORT": "3311",
                "DB_USERNAME": "root",
                "DB_PASSWORD": "root",
            },
            "red": {
                "OS_HOST": "127.0.0.1",
                "DB_HOST": "127.0.0.1",
                "DB_PORT": "3312",
                "DB_USERNAME": "root",
                "DB_PASSWORD": "root",
            }
        }
    }

    def __init__(self, src: str, dst: str):
        # self.services = {
        #     # "log": self.log,
        #     # "file": FileOperate(),
        #     # "pg": PgUtil(self.env, self.log),
        #     "connections": self.connections,
        #     "env": self.env
        # }

        self.src = src
        self.dst = dst
        self.route()

    def pnr(self, anything):
        print(anything)
        return anything

    def route(self):
        print("action == {action}".format(action="gag"))
        self.switch_to_active()

        # if self.src == self.dst:
        #     print("ERROR: src == dst")
        #     return
        #
        # if self.src == "dr":
        #     self.switch_to_active()
        # elif self.dst == "dr":
        #     self.switch_to_dr()

    def tunnel_exec(self, env: str, service: str, cmd: str) -> str:
        e: dict = self.connections[env][service]

        DEBUG: bool = True

        tunnel_cmd = self.pnr("""
ssh -L 0.0.0.0:3306:{OS_HOST}:{DB_PORT} {ssh_host} `# {env}-mysql-{service}` -T '{mysql_bin} -h {DB_HOST} -u {DB_USERNAME} -p{DB_PASSWORD} -P {DB_PORT} -e \"{cmd}\"';
        """.format(
            env=env,
            service=service,
            OS_HOST=e["OS_HOST"],
            DB_HOST=e["DB_HOST"],
            DB_PORT=e["DB_PORT"],
            DB_USERNAME=e["DB_USERNAME"],
            DB_PASSWORD=e["DB_PASSWORD"],
            cmd=cmd,
            ssh_host="myself" if e["OS_HOST"] == "127.0.0.1" else "carrefour-bastion",
            mysql_bin="/usr/local/opt/mysql-client/bin/mysql" if e["OS_HOST"] == "127.0.0.1" else "/usr/bin/mysql"
        ))

        # if not DEBUG:
        #     os.system(tunnel_cmd)

    def show_mysql_log_file_last(self):
        print("""
        MASTER_LOG_FILE='mysql-bin.000003',
        MASTER_LOG_POS=120;
        """)

    def switch_to_dr(self):
        # === CQRS ===
        print("srt == {}".format(self.src))
        print("dst == {}".format(self.dst))

        # dr
        for service in ["admin", "app", "red"]:
            self.tunnel_exec(self.dst, service, cmd="SET GLOBAL read_only=0;SHOW GLOBAL VARIABLES LIKE \"%read_only%\";START SLAVE;")

        # active
        for service in ["admin", "app", "red"]:
            self.tunnel_exec(self.src, service, cmd="SET GLOBAL read_only=1;SHOW GLOBAL VARIABLES LIKE \"%read_only%\";START SLAVE;")

        # === replicate ===
        active = self.src
        for service in ["admin", "app", "red"]:
            service = service
            self.tunnel_exec(self.dst, service, cmd="""
                # master
                CREATE USER '{client_DB_USERNAME}'@'%' IDENTIFIED BY '{client_DB_PASSWORD}';
                GRANT REPLICATION SLAVE ON *.* TO '{client_DB_USERNAME}'@'%';
                FLUSH PRIVILEGES;
            """.format(
                client_DB_HOST=self.connections[active][service]["DB_HOST"],
                client_DB_USERNAME=self.connections[active][service]["DB_USERNAME"],
                client_DB_PASSWORD=self.connections[active][service]["DB_PASSWORD"],
            ))

            # getMASTER_STATUS;
            self.tunnel_exec(self.dst, service, cmd="SHOW MASTER STATUS;".format())

            self.tunnel_exec(active, service, cmd="""
                # slave
                SLAVE STOP;
                CHANGE MASTER TO 
                MASTER_HOST='{DB_HOST}', 
                MASTER_USER='{DB_USERNAME}', 
                MASTER_PASSWORD='{DB_PASSWORD}',
                MASTER_LOG_FILE='mysql-bin.000003',
                MASTER_LOG_POS=120;
                SLAVE START;
            """.format(
                DB_HOST=self.connections[self.dst][service]["DB_HOST"],
                DB_USERNAME=self.connections[self.dst][service]["DB_USERNAME"],
                DB_PASSWORD=self.connections[self.dst][service]["DB_PASSWORD"],
            ))

    def switch_to_active(self):
        # === CQRS ===
        print("srt == {}".format(self.src))
        print("dst == {}".format(self.dst))

        # dr
        for service in ["admin", "app", "red"]:
            self.tunnel_exec(self.src, service, cmd="SET GLOBAL read_only=0;SHOW GLOBAL VARIABLES LIKE \"%read_only%\";START SLAVE;")

        # active
        for service in ["admin", "app", "red"]:
            self.tunnel_exec(self.dst, service, cmd="SET GLOBAL read_only=1;SHOW GLOBAL VARIABLES LIKE \"%read_only%\";START SLAVE;")

        # === replicate ===
        active = self.dst
        for service in ["admin", "app", "red"]:
            service = service
            self.tunnel_exec(self.src, service, cmd="""
                # master
                CREATE USER '{client_DB_USERNAME}'@'%' IDENTIFIED BY '{client_DB_PASSWORD}';
                GRANT REPLICATION SLAVE ON *.* TO '{client_DB_USERNAME}'@'%';
                FLUSH PRIVILEGES;
            """.format(
                client_DB_HOST=self.connections[active][service]["DB_HOST"],
                client_DB_USERNAME=self.connections[active][service]["DB_USERNAME"],
                client_DB_PASSWORD=self.connections[active][service]["DB_PASSWORD"],
            ))

            # getMASTER_STATUS;
            self.tunnel_exec(self.src, service, cmd="SHOW MASTER STATUS;".format())

            self.tunnel_exec(active, service, cmd="""
                # slave
                CHANGE MASTER TO MASTER_HOST='{DB_HOST}', MASTER_USER='{DB_USERNAME}', MASTER_PASSWORD='{DB_PASSWORD}',
                MASTER_LOG_FILE='mysql-bin.000003',
                MASTER_LOG_POS=120;
                SLAVE START;
            """.format(
                DB_HOST=self.connections[self.src][service]["DB_HOST"],
                DB_USERNAME=self.connections[self.src][service]["DB_USERNAME"],
                DB_PASSWORD=self.connections[self.src][service]["DB_PASSWORD"],
            ))

    def show_info(self):
        for env in ["sit", "uat", "dr"]:
            for service in ["admin", "app", "red"]:
                self.tunnel_exec(env, service, """
                    SELECT 1;
                    SHOW MASTER STATUS;
                    SHOW SLAVE STATUS;
                    SHOW GLOBAL VARIABLES;
                """)

    def get_master_status(self, key:str):
        return


if __name__ == "__main__":
    for i in range(1, len(sys.argv)):
        print("args[{}] = {}".format(str(i), sys.argv[i]))

    try:
        etl = ETL(
            src=sys.argv[1],
            dst=sys.argv[2]
        )
    except Exception as e:
        # os.system("python3.7 cmd/list_case.py")
        print("Error: Please check arguments. \n{}".format(str(e)))

    try:
        etl.route()
    except Exception as e:
        print("Error: {}".format(str(e)))
        # etl.log.error_stack(e)
    finally:
        sys.exit(1)
