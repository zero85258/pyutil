import os
import traceback
from datetime import datetime


class LogUtil:
    def __init__(self, path: str = "./logs/default.log") -> None:
        # self.io = open(
        #     "{logFolder}{table}_{curr_timestamp}.log".format(
        #         logFolder=log_folder, table=table,
        #         curr_timestamp=get_today(the_date=datetime.now(), the_format="%Y%m%d_%H%M%S")
        #     ), "a"
        # )

        self._mkdir(path)

    def info(self, msg: str, path: str = "./logs/default.log") -> None:
        self._mkdir(path)

        msg = "Info: {timeStamp}: {msg}\n".format(
            timeStamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            msg=msg
        )

        self._write_and_print(msg)

    def print_and_log(self, msg: str) -> None:
        print(msg)
        self.info(msg)

    def error_stack(self, e: Exception, path: str = "./logs/error.log") -> None:
        self._mkdir(path)
        msg = "Error: {timeStamp}: {stackMsg}".format(
            timeStamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            stackMsg=''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
        )

        self._write_and_print(msg)

    def error(self, msg: str, path: str = "./logs/error.log") -> None:
        self._mkdir(path)
        msg = "Error: {timeStamp}: {msg}\n".format(
            timeStamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            msg=msg
        )

        self._write_and_print(msg)

    def _write_and_print(self, msg: str) -> None:
        # print("{msg}\n".format(msg=msg))
        self.io.write(msg)
        self.io.flush()

    def _mkdir(self, path: str) -> None:
        self.path = path

        dir_path, file = os.path.split(path)
        if not os.path.isdir(dir_path):
            os.makedirs(dir_path)

        self.io = open("{path}".format(path=self.path), "a")

    def close_file(self) -> None:
        self.io.flush()
        self.io.close()
