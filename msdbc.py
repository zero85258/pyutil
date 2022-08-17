# -*- coding: utf8 -*-
"""
主要存放 對資料庫的新增，刪除，修改，還有取得連線資訊
(取得connection ，關閉connection，select，update，insert)的共用方法
"""
from typing import List, Optional, Tuple
import time
import csv
import pymssql
from util.log import LogUtil


class MSDBC:
    """
    主要存放 對資料庫的新增，刪除，修改，還有取得連線資訊
    (取得connection ，關閉connection，select，update，insert)的共用方法

        :Attributes:
        db: database name
        user: user name
        pwd: password
        host: host IP
    """

    def __init__(self, log: LogUtil, host: str, db: str, user: str, pwd: str) -> None:
        self._conn = None
        self._cur = None
        self._server: str = host
        self._db: str = db
        self._user: str = user
        self._pwd: str = pwd
        self._log: LogUtil = log

        self.open_conn()
        self._log.info("connection :D")

    def is_table_exist(self, schema_name: str, table_name: str) -> bool:
        """
        如果執行失敗拋回exception e
        """
        self._log.info("Enter if_table_exist() function")
        try:
            if not self._conn:
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
            fo = self._cur.fetchone()
            self._log.info("execute sql successful")
            if fo:
                table_cnt = int(fo[0])
            else:
                table_cnt = -1

            self._log.info("Exit if_table_exist() normally.")
        except Exception as e:
            # self._log.error_stack(e)
            raise Exception(str(e))

        return True if table_cnt > 0 else False

    def statement(self, sql: str) -> int:
        """
        該方法主要提供執行 INSERT(SQL) 的指令, 回傳的int代表受影響的資料數,
        若SQL執行錯誤則回傳exception

        :param sql: 字串SQL指令
        :return: result: 資料更新影響的行數
        """
        self._log.info("Enter Insert() function")

        try:
            if not self._conn:
                self.open_conn_with_retry(5)
            if not self._cur:
                self._cur = self._conn.cursor()

            # self._conn.autocommit = False
            self._log.info("Begin execute sql = \n" + sql + "\n")
            self._cur.execute(sql)
            self._log.info(str(self._cur.rowcount) + " records was inserted successfully!")
            self._conn.commit()

            # self._conn.autocommit = True
            self._log.info("Exit insert() normally.")
        except Exception as e:
            # self._log.info(str(e))
            # self._log.error_stack(e)
            # self._log.info("start conn.rollback.")
            self._conn.rollback()
            # self._conn.autocommit = True
            raise Exception(str(e))

        return self._cur.rowcount

    def query(self, sql: str) -> List[Tuple]:
        """
        該方法主要提供執行 SELECT(SQL) 的指令,回傳的: ResultSet代表返回的多筆結果
        若SQL執行錯誤則回傳exception
        """
        self._log.info("Enter select() function")

        try:
            if not self._conn:
                self.open_conn_with_retry(5)
            if not self._cur:
                self._cur = self._conn.cursor()

            self._log.info("Begin execute sql = \n" + sql + "\n")
            self._cur.execute(sql)
            result: list = self._cur.fetchall()

            self._log.info(str(self._cur.rowcount) + " records was selected successfully!")
            self._log.info("execute sql successful")
            self._log.info("Exit select() normally.")
        except Exception as e:
            self._log.error_stack(e)
            raise Exception(str(e))

        return result

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
                self._log.info("connection關閉")

        except Exception as e:
            # self._log.info(str(e))
            self._log.error_stack(e)
            raise Exception(str(e))

    def open_conn(self):
        try:
            if not self._conn:
                self._conn = pymssql.connect(
                    server=self._server,
                    database=self._db,
                    user=self._user,
                    password=self._pwd
                )

            return self
        except Exception as e:
            self._log.error_stack(e)
            raise Exception(str(e))

    #
    # def set_schema(self, schema: str):
    #     """
    #     登入 odbc 的連線拿到該Table的 Connection
    #     """
    #
    #     try:
    #         self._conn = pyodbc.connect(
    #             driver=self._driver,
    #             trusted_connection='no',
    #             server=self._server,
    #             database=self._db,
    #             UID=self._user,
    #             PWD=self._pwd,
    #         )
    #
    #         # self._conn = pyodbc.connect("""
    #         #         DRIVER={driver};SERVER={server};DATABASE={db};UID={user};PWD={pwd}
    #         #     """.format(
    #         #     driver=self._driver,
    #         #     server=self._server,
    #         #     db=self._db,
    #         #     user=self._user,
    #         #     pwd=self._pwd
    #         # ))
    #         # self._conn.autocommit = True
    #         self._log.info("Connection to ODBC established: " + self._server + "/" + self._db + "\n")
    #
    #         return self
    #     except pyodbc.Error as e:
    #         self._log.error_stack(e)
    #         raise Exception(str(e))

    def open_conn_with_retry(self, cnt: int) -> None:
        """
        判斷Connection狀態(是否關閉或是null)與方法執行次數(小於可嘗試次數),
        以取得Connection.

        :param cnt: 嘗試登入次數
        :return:
        """
        run_seq: int = 1
        while (not self._conn) and run_seq <= cnt:
            try:
                self.open_conn()

                self._log.info(
                    "Connection to {} established: {} {}".format(
                        self.__class__.__name__,
                        self._server,
                        self._db
                    )
                )
            except Exception as e:
                self._log.info(
                    "Fail to connect to {} in {} st run.\n".format(
                        self.__class__.__name__,
                        str(run_seq)
                    )
                )
                self._log.error_stack(e)
                time.sleep(5)
            finally:
                run_seq += 1

    def export_csv(self, sql: str, path: str) -> None:
        """
        該方法主要提供執行 SELECT(SQL) 的指令,回傳的: ResultSet代表返回的多筆結果
        若SQL執行錯誤則回傳exception
        """
        self._log.info("Enter export_csv() function")

        try:
            if not self._conn:
                self.open_conn_with_retry(5)
            if not self._cur:
                self._cur = self._conn.cursor()

            self._log.info("Begin execute sql = \n" + sql + "\n")
            self._cur.execute(sql)

            with open(path, "w", newline='') as fp:
                writer = csv.writer(fp, quoting=csv.QUOTE_NONNUMERIC)

                for row in self._cur:
                    writer.writerow(row)

            self._log.info(str(self._cur.rowcount) + " records was selected successfully!")
            self._log.info("execute sql successful")
            self._log.info("Exit select() normally.")
        except Exception as e:
            self._log.error_stack(e)
            raise Exception(str(e))

    def invoke_store_procedural(self, function: str) -> bool:
        try:
            if not self._conn:
                self.open_conn_with_retry(5)
            if not self._cur:
                self._cur = self._conn.cursor()

            self._log.info("Begin callproc sql = \n" + function + "\n")
            self._cur.callproc(function)
            self._conn.commit()

            self._log.info("execute function successful")
            self._log.info("Exit invoke_store_procedural() normally.")
        except Exception as e:
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
    #     except pyodbc.Error as e:
    #         self._log.error(e)
    #         raise Exception(str(e))
    #
    #     return result
