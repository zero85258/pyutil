# -*- coding: utf8 -*-
"""
主要存放 對資料庫的新增，刪除，修改，還有取得連線資訊
(取得connection ，關閉connection，select，update，insert)的共用方法

    Typical usage example:
    di = Connection()
    di.select(sel_sql)
    di.close_conn()

    psycopg2.Connection.closed: 0 if the connection is open,
    nonzero if it is closed or broken.
"""
from typing import List, Optional, Tuple
from util.log import LogUtil
import subprocess
import time
import psycopg2


class PGDBC:
    """
    主要存放 對資料庫的新增，刪除，修改，還有取得連線資訊
    (取得connection ，關閉connection，select，update，insert)的共用方法

        :Attributes:
        db_name: database name
        user: user name
        pwd: password
        host: host IP
    """

    def __init__(self, log: LogUtil, host: str, db_name: str, user: str, pwd: str) -> None:
        self._conn = None  # psycopg2.connection
        self._cur = None  # psycopg2.cursor
        self._db_name: str = db_name
        self._user: str = user
        self._pwd: str = pwd
        self._host: str = host
        self._log: LogUtil = log

        self.open_conn()

    def is_table_exist(self, schema_name: str, table_name: str) -> bool:
        """

        如果執行失敗拋回exception e

        :param schema_name: 資料庫名稱
        :param table_name :  資料表名稱
        :return: table_cnt: 回傳-1代表該Table 不存在, 回傳1代表該Table存在
        """
        self._log.info("Enter if_table_exist() function")
        try:
            if (not self._conn) or self._conn.closed != 0:
                self.open_conn_with_retry(5)
            if not self._cur:
                self._cur = self._conn.cursor()

            sql: str = (
                    "SELECT EXISTS (SELECT * FROM pg_catalog.pg_tables\n" +
                    "WHERE schemaname = '" + schema_name + "'\n" +
                    "AND tablename = '" + table_name + "');\n"
            )

            self._log.info("execute sql = \n" + sql + "\n")
            self._cur.execute(sql)
            fo = self._cur.fetchone()  # type: tuple
            self._log.info("execute sql successful")
            if fo:
                table_cnt = int(fo[0])
            else:
                table_cnt = -1

            self._log.info("Exit if_table_exist() normally.")
        except psycopg2.Error as e:
            # self._log.error_stack(e)
            raise Exception(str(e))

        return True if table_cnt > 0 else False

    def get_table_ddl(self, schema: str, table: str) -> str:
        """
        執行getSchemaSql,得到一組 Table的 SchemaDDL ,如果該 Schema有效則回傳該Schema ,若無效則丟回exception

        :param schema:
        :param table: 來源資料表
        :return: String schema: SchemaDDL
        """
        self._log.info("Enter get_table_ddl()")

        try:
            ddl = self.pg_dump_sql(
                schema, table, "--schema-only --no-owner --no-privileges --gp-syntax"
            )

            self._log.info("exit get_table_ddl() normally")
            return ddl
        except Exception as e:
            raise Exception(str(e))

    def query(self, sql: str) -> List[Tuple]:
        """
        該方法主要提供執行 SELECT(SQL) 的指令,回傳的: ResultSet代表返回的多筆結果
        若SQL執行錯誤則回傳exception
        """
        self._log.info("Enter select() function")

        try:
            if (not self._conn) or self._conn.closed != 0:
                self.open_conn_with_retry(5)
            if not self._cur:
                self._cur = self._conn.cursor()

            self._log.info("Begin execute sql = \n" + sql + "\n")
            self._cur.execute(sql)
            result_list: List[Tuple] = self._cur.fetchall()

            self._log.info(str(self._cur.rowcount) + " records was selected successfully!")
            self._log.info("execute sql successful")
            self._log.info("Exit select() normally.")
        except psycopg2.Error as e:
            self._log.error_stack(e)
            raise Exception(str(e))

        return result_list

    def statement(self, sql: str) -> int:
        """
        該方法主要提供執行 UPDATE(SQL) 的指令, 回傳的int代表受影響的資料數,
        若SQL執行錯誤則回傳exception

        :param sql: 字串SQL指令
        :return: result: 資料更新影響的行數
        """
        self._log.info("Enter statement() function")

        try:
            if (not self._conn) or self._conn.closed != 0:
                self.open_conn_with_retry(5)
            if not self._cur:
                self._cur = self._conn.cursor()

            # self._conn.autocommit = False
            self._log.info("Begin execute sql = \n" + sql + "\n")
            self._cur.execute(sql)
            self._log.info("Executed sql=\n" + self._cur.query.decode("utf-8"))
            self._log.info(str(self._cur.rowcount) + " records was updated successfully!")
            self._log.info("execute sql successful")
            self._conn.commit()

            # self._conn.autocommit = True
            self._log.info("Exit statement() normally.")
        except psycopg2.Error as e:
            # self._log.error_stack(e)
            # self._log.info("start conn.rollback.")
            self._conn.rollback()
            # self._conn.autocommit = True
            raise Exception(str(e))

        return self._cur.rowcount

    def close_conn(self) -> None:
        """
        close()方法以關閉連線
        關閉連線資訊 (connection,prepareStatement ...)

        :return:
        """
        self._log.info("begin close_conn()")
        try:
            if self._cur:
                self._cur.close()

            if self._conn:
                self._conn.close()
                # print("connection關閉")
                self._log.info("connection關閉")

        except psycopg2.Error as e:
            # self._log.info(str(e))
            self._log.error_stack(e)
            raise Exception(str(e))

    def open_conn(self):
        """
        登入greenplum的連線拿到該Table的 Connection

        :return:
        """
        if not self._conn:
            self._conn = psycopg2.connect(
                dbname=self._db_name,
                user=self._user,
                password=self._pwd,
                host=self._host
            )
            # self._conn.autocommit = True
            self._log.info("Connection to Greenplum established: " + self._host + "/" + self._db_name + "\n")

        return self

    def set_schema(self, schema: str):
        """
        登入greenplum的連線拿到該Table的 Connection

        :return:
        """

        self._conn = psycopg2.connect(
            dbname=self._db_name,
            user=self._user,
            password=self._pwd,
            host=self._host,
            options=f'-c search_path={schema}',
        )
        # self._conn.autocommit = True
        self._log.info("Connection to Greenplum established: " + self._host + "/" + self._db_name + "\n")

        return self

    def open_conn_with_retry(self, cnt: int) -> None:
        """
        判斷Connection狀態(是否關閉或是null)與方法執行次數(小於可嘗試次數),
        以取得Connection.

        :param cnt: 嘗試登入次數
        :return:
        """
        run_seq = 1  # type: int
        while ((not self._conn) or self._conn.closed != 0) and run_seq <= cnt:
            try:
                self.open_conn()

                self._log.info(
                    " Connection to Greenplum established: " + self._host + "/" + self._db_name + "\n"
                )
            except psycopg2.Error as e:
                self._log.info(
                    " Fail to connect to Greenplum in " + str(run_seq) + "st run.\n"
                )
                self._log.error_stack(e)
                time.sleep(5)
            finally:
                run_seq += 1

    def pg_dump_sql(self, schema: str, table: str, options: Optional[str] = "") -> str:
        """
        執行getSchemaSql,得到一組 Table的 SchemaDDL ,如果該 Schema有效則回傳該Schema ,若無效則丟回exception

        :param schema:
        :param table: 來源資料表
        :param options:
        :return: String schema: SchemaDDL
        """
        if options and options[0] != " ":
            options = " " + options

        cmd: str = (
                "pg_dump --dbname=" + self._db_name +
                " --host=" + self._host +
                " --username=" + self._user +
                " --no-password --table=" + schema + "." + table + options
        )

        try:
            self._log.info("Execute cmd:\n" + cmd)
            sql = subprocess.getoutput(cmd)  # type: str

            self._log.info("pg_dump_sql sql=\n" + sql)
            self._log.info("exit pg_dump_sql() normally")
            return sql
        except Exception as e:
            raise e

    def invoke_store_procedural(self, function: str) -> bool:
        self._log.info("Enter invoke_store_procedural function")

        try:
            if (not self._conn) or self._conn.closed != 0:
                self.open_conn_with_retry(5)
            if not self._cur:
                self._cur = self._conn.cursor()

            self._log.info("Begin callproc sql = \n" + function + "\n")
            self._cur.callproc(function)
            self._conn.commit()

            self._log.info("execute function successful")
            self._log.info("Exit invoke_store_procedural() normally.")
        except psycopg2.Error as e:
            # self._log.error(e)
            raise Exception(str(e))

        return True

    # def invoke_store_procedural_with_parameters(self, function: str, *parameters):
    #     self._log.info("Enter invoke_store_procedural function")
    #
    #     try:
    #         if (not self._conn) or self._conn.closed != 0:
    #             self.open_conn_with_retry(5)
    #         if not self._cur:
    #             self._cur = self._conn.cursor()
    #
    #         self._log.info("Begin callproc sql = \n" + function + "\n")
    #         self._cur.callproc(function, parameters)
    #         result = self._cur.fetchone()
    #         print(result)
    #         self._conn.commit()
    #
    #         self._log.info("execute function successful")
    #         self._log.info("Exit invoke_store_procedural() normally.")
    #     except psycopg2.Error as e:
    #         self._log.error(e)
    #         raise Exception(str(e))
    #
    #     return result
