from __future__ import annotations

import datetime
import glob
import json
import os
import statistics
from enum import Enum

import pygit2 as pygit2


class DataType:
    """
    DataType is a representation of a weather data type
    """

    def __init__(self, symbol: str, unit: str, fileName: str, italianName: str, precision: int = 2):
        """
         Construct a new 'DataType' object.
        :param symbol: The internal data symbol of the value
        :param unit: The unit of measurement
        :param fileName: The CSV associated filename in https://github.com/StazioneMeteoCocito/dati
        :param italianName: The italian name of the data type
        :return: returns nothing
        """
        self.symbol = symbol
        self.unit = unit
        self.fileName = fileName
        self.italianName = italianName
        self.precision = precision

    def __str__(self) -> str:
        return self.italianName + ", espresso in " + self.unit + ", file di tipo " + self.fileName + ", simbolo " + self.symbol + ", precisione: " + str(
            self.precision)

    def __eq__(self, other: DataType) -> bool:
        return self.symbol == other.symbol


class DataTypeArchive:
    """
    A class to handle the list of available datatypes
    """
    data = [
        DataType("T", "°C", "temperature.csv", "Temperatura"),
        DataType("H", "%", "humidity.csv", "Umidità"),
        DataType("P", "hPa", "pressure.csv", "Pressione"),
        DataType("PM10", "µg/m³", "pm10.csv", "PM10"),
        DataType("PM25", "µg/m³", "pm25.csv", "PM2,5"),
        DataType("S", "µg/m³", "smoke.csv", "Fumo e vapori infiammabili")
    ]

    def __len__(self) -> int:
        return len(self.data)

    def __str__(self) -> str:
        st = "Archivio di tipologie di Dati\n"
        for dt in self.data:
            st += "* " + str(dt) + "\n"
        return st

    class Symbols(Enum):
        """
        List of internal symbols associated with datatypes
        """
        temperature = "T"
        humidity = "H"
        pressure = "P"
        pm10 = "PM10"
        pm25 = "PM25"
        smoke = "S"

    @staticmethod
    def fromSymbol(symbol: str) -> DataType | None:
        """
        Obtain DataType from Symbol
        :param symbol: The symbol
        :return: The DataType
        """
        for dataType in DataTypeArchive.data:
            if dataType.symbol == symbol:
                return dataType
        return None

    @staticmethod
    def fromUnit(unit: str) -> DataType | None:
        """
        Obtain DataType from Unit
        :param unit: The Unit
        :return: The DataType
        """
        for dataType in DataTypeArchive.data:
            if dataType.unit == unit:
                return dataType
        return None

    @staticmethod
    def fromFileName(fileName: str) -> DataType | None:
        """
        Obtain DataType from fileName
        :param fileName: The fileName
        :return: The DataType
        """
        for dataType in DataTypeArchive.data:
            if dataType.fileName == fileName:
                return dataType
        return None

    @staticmethod
    def fromItalianName(italianName: str) -> DataType | None:
        """
        Obtain DataType from ItalianName
        :param italianName: The ItalianName
        :return: The DataType
        """
        for dataType in DataTypeArchive.data:
            if dataType.italianName == italianName:
                return dataType
        return None


class Value:
    """
    Stores a value, its datatype symbol and instant of acquisition
    """

    def __init__(self, value: float, symbol: DataTypeArchive.Symbols = DataTypeArchive.Symbols.temperature,
                 instant: datetime.datetime = datetime.datetime.now()):
        self.value = round(value, DataTypeArchive.fromSymbol(symbol.value).precision)
        self.symbol = symbol
        self.instant = instant

    def __int__(self) -> int:
        return int(self.value)

    def __float__(self) -> float:
        return self.value


class DataArchive:


    @staticmethod
    def create() -> None:
        """
        Download the data the first time
        :return: None
        :except May rise connection exceptions
        """
        if not os.path.isdir("dati/.git"):
            pygit2.clone_repository("https://github.com/StazioneMeteoCocito/dati", "dati")
        else:
            DataArchive.update()


    @staticmethod
    def __pull(repo, remote_name='origin') -> None:
        """
        Pull data in a repository, internal utility
        :param repo: repository local path
        :param remote_name: the name of the remote
        :return: None
        """
        for remote in repo.remotes:
            if remote.name == remote_name:
                remote.fetch()
                remote_master_id = repo.lookup_reference('refs/remotes/origin/main').target
                merge_result, _ = repo.merge_analysis(remote_master_id)
                # Up to date, do nothing
                if merge_result & pygit2.GIT_MERGE_ANALYSIS_UP_TO_DATE:
                    return
                # We can just fastforward
                else:
                    master_ref = repo.lookup_reference('refs/heads/main')
                    master_ref.set_target(remote_master_id)
                    # We just yeet changes away
                    repo.reset(master_ref.target, pygit2.GIT_RESET_HARD)
    @staticmethod
    def update() -> None:
        """
        Pull data
        :return: None
        :except May rise connection exceptions
        """
        if not os.path.isdir("dati/.git"):
            DataArchive.create()
        else:
            DataArchive.__pull(pygit2.Repository("dati"))

    @staticmethod
    def report() -> str:
        """
        Obtain last hardware report text
        """
        with open("dati/report.txt") as r:
            return r.read()

    @staticmethod
    def current() -> dict:
        """
        Obtain the last data
        """
        with open("dati/last.json") as r:
            return json.loads(r.read())

    @staticmethod
    def __lastPathElement(path: str) -> str:
        """
        Obtain the last element of a path
        :param path: The path
        :return: the last element
        """
        return path.split("/")[-1]

    @staticmethod
    def betweenDatetimes(start: datetime.datetime, end: datetime.datetime) -> list[Value]:
        """
        Obtain a list of all values between two datetimes
        :param start: start datetime
        :param end: end datetime
        :return: lsit of Values
        """
        list = []
        for yPath in glob.glob("dati/2*"):
            year = int(DataArchive.__lastPathElement(yPath))
            for mPath in glob.glob(yPath + "/*"):
                month = int(DataArchive.__lastPathElement(mPath))
                for dPath in glob.glob(mPath + "/*"):
                    day = int(DataArchive.__lastPathElement(dPath))
                    pivot = datetime.datetime.strptime(
                        str(year).zfill(4) + "-" + str(month).zfill(2) + "-" + str(day).zfill(2), "%Y-%m-%d")
                    if start > pivot or pivot > end:
                        continue
                    for elementPath in glob.glob(dPath + "/*.csv"):
                        lep = DataArchive.__lastPathElement(elementPath)
                        type = DataTypeArchive.Symbols(DataTypeArchive.fromFileName(lep).symbol)
                        with open(elementPath, "r") as f:
                            for line in f.readlines():
                                l = line.split(",")
                                if len(l) < 2:
                                    continue
                                dateT = datetime.datetime.strptime(l[0], '%Y-%m-%d %H:%M:%S')
                                if start > dateT or end < dateT:  # It should not be necessary, but I guess
                                    continue
                                list.append(Value(float(l[1]), type, dateT))
        return list

    @staticmethod
    def day() -> list[Value]:
        """
        Obtain all values generated today
        :return: list of values
        """
        start = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        end = datetime.datetime.now()
        return DataArchive.betweenDatetimes(start, end)

    @staticmethod
    def week() -> list[Value]:
        """
        Obtain all values generated this week
        :return: list of values
        """
        now = datetime.datetime.now()
        monday = (now - datetime.timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        return DataArchive.betweenDatetimes(monday, now)

    @staticmethod
    def month() -> list[Value]:
        """
        Obtain all values generated this month
        :return: list of values
        """
        start = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0, day=1)
        end = datetime.datetime.now()
        return DataArchive.betweenDatetimes(start, end)

    @staticmethod
    def latestDatetime() -> str:
        """
        Obtain a string representation (dd/mm/yyyy HH:MM:SS) of the last data instant
        :return: The string representation
        """
        mY = int(datetime.datetime.now().strftime("%Y"))
        mM = 12
        mD = 31
        tdate = ""
        for i in range(mY):
            yPath = "dati/" + str(mY - i)
            if os.path.isdir(yPath):
                for j in range(mM):
                    mPath = yPath + "/" + str(mM - j).zfill(2)
                    if os.path.isdir(mPath):
                        for k in range(mD):
                            dPath = mPath + "/" + str(mD - k).zfill(2)
                            if os.path.isdir(dPath):
                                tfile = open(dPath + "/temperature.csv", "r")
                                lines = tfile.readlines()
                                for line in lines:
                                    r = line.split(",")
                                    if len(r) < 2:
                                        continue
                                    tdate = r[0]
                                if len(tdate) == 0:
                                    return datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                                else:
                                    return datetime.datetime.strptime(tdate, "%Y-%m-%d %H:%M:%S").strftime(
                                        "%d/%m/%Y %H:%M:%S")


class Stats:
    """
    Takes a datalist and sets a "results" resultMap attribute with a statistical value for each symbol
    """

    def __init__(self, dataList: list):
        """
        Instantiate a Stat Object
            :param dataList: The target datalist
        """
        tempMap = {}
        resultMap = {}
        for el in dataList:
            if el.symbol not in tempMap.keys():
                tempMap[el.symbol] = {"list": [], "iList": []}
            if el.symbol not in resultMap.keys():
                resultMap[el.symbol] = {"max": Value(-10E6), "min": Value(10E6), "itemCount": 0}

            if el.value > resultMap[el.symbol]["max"].value:
                resultMap[el.symbol]["max"] = el
            elif el.value < resultMap[el.symbol]["min"].value:
                resultMap[el.symbol]["min"] = el

            tempMap[el.symbol]["list"].append(el.value)
            tempMap[el.symbol]["iList"].append(int(el.value))
            resultMap[el.symbol]["itemCount"] += 1

        for symbol in resultMap.keys():
            resultMap[symbol]["stdev"] = statistics.stdev(tempMap[symbol]["list"])
            resultMap[symbol]["mean"] = statistics.mean(tempMap[symbol]["list"])
            resultMap[symbol]["mode"] = float(statistics.mean(tempMap[symbol]["iList"]))

        self.results = resultMap


class TextGenerator:
    """
        Generatior for italian text excerpts representing the data
    """

    @staticmethod
    def current() -> list[str]:
        """
        Current data excerpt
        :return: array of excerpts
        """
        ldt = DataArchive.latestDatetime()
        c = DataArchive.current()
        return ["Dati Meteorologici:\nUltimo aggiornamento: " + ldt + "\n--------------\nTemperatura: " + (
            "{:.2f}".format(c["T"])) + " °C\n" + "Umidità: " + (
                    "{:.2f}".format(c["H"])) + " %\n" + "Pressione: " + (
                    "{:.2f}".format(c["P"])) + " hPa\n" + "PM10: " + (
                    "{:.2f}".format(c["PM10"])) + " µg/m³\n" + "PM2,5: " + (
                    "{:.2f}".format(c["PM25"])) + " µg/m³\n" + "Fumo e vapori infiammabili: " + (
                    "{:.2f}".format(c["S"])) + " µg/m³"]

    @staticmethod
    def report() -> list[str]:
        """
        Current hardware report
        :return: array of excerpts
        """
        return [DataArchive.report()]

    @staticmethod
    def week() -> list[str]:
        """
        Current week summary
        :return: array of excerpts
        """
        i = 0
        list = []
        s = Stats(DataArchive.week()).results
        l = len(s.keys())
        for symbol in s.keys():
            dta = DataTypeArchive.fromSymbol(symbol.value)
            ssm = s[symbol]
            list.append(
                "(" + str(i + 1) + "/" + str(
                    l) + ") Dati di questa settimana\n---" + dta.italianName + "---\nMedia: " + (
                    "{:.2f}".format(ssm["mean"])) + " " + dta.unit + "\nModa: " + (
                    "{:.2f}".format(ssm["mode"])) + " " + dta.unit + "\nMassimo: " + (
                    "{:.2f}".format(ssm["max"].value)) + " " + dta.unit + " (" + ssm["max"].instant.strftime(
                    "%d/%m/%Y %H:%M:%S") + ") " + "\nMinimo: " + (
                    "{:.2f}".format(ssm["min"].value)) + " " + dta.unit + " (" + ssm["min"].instant.strftime(
                    "%d/%m/%Y %H:%M:%S") + ") " + "\nDeviazione Standard: " + (
                    "{:.2f}".format(ssm["stdev"])) + " " + dta.unit + "\nModa: " + (
                    "{:.2f}".format(ssm["mean"])) + " " + dta.unit + "\nNumero di rilevazioni: " + (
                    "{:.2f}".format(ssm["itemCount"])))
            i += 1
        return list

    @staticmethod
    def month() -> list[str]:
        """
        Current month summary
        :return: array of excerpts
        """
        i = 0
        list = []
        s = Stats(DataArchive.month()).results
        l = len(s.keys())
        for symbol in s.keys():
            dta = DataTypeArchive.fromSymbol(symbol.value)
            ssm = s[symbol]
            list.append(
                "(" + str(i + 1) + "/" + str(l) + ") Dati di questo mese\n---" + dta.italianName + "---\nMedia: " + (
                    "{:.2f}".format(ssm["mean"])) + " " + dta.unit + "\nModa: " + (
                    "{:.2f}".format(ssm["mode"])) + " " + dta.unit + "\nMassimo: " + (
                    "{:.2f}".format(ssm["max"].value)) + " " + dta.unit + " (" + ssm["max"].instant.strftime(
                    "%d/%m/%Y %H:%M:%S") + ") " + "\nMinimo: " + (
                    "{:.2f}".format(ssm["min"].value)) + " " + dta.unit + " (" + ssm["min"].instant.strftime(
                    "%d/%m/%Y %H:%M:%S") + ") " + "\nDeviazione Standard: " + (
                    "{:.2f}".format(ssm["stdev"])) + " " + dta.unit + "\nModa: " + (
                    "{:.2f}".format(ssm["mean"])) + " " + dta.unit + "\nNumero di rilevazioni: " + (
                    "{:.2f}".format(ssm["itemCount"])))
            i += 1
        return list

    @staticmethod
    def day() -> list[str]:
        """
        Current day summary
        :return: array of excerpts
        """
        i = 0
        list = []
        s = Stats(DataArchive.day()).results
        l = len(s.keys())
        for symbol in s.keys():
            dta = DataTypeArchive.fromSymbol(symbol.value)
            ssm = s[symbol]
            list.append(
                "(" + str(i + 1) + "/" + str(l) + ") Dati di oggi\n---" + dta.italianName + "---\nMedia: " + (
                    "{:.2f}".format(ssm["mean"])) + " " + dta.unit + "\nModa: " + (
                    "{:.2f}".format(ssm["mode"])) + " " + dta.unit + "\nMassimo: " + (
                    "{:.2f}".format(ssm["max"].value)) + " " + dta.unit + " (" + ssm["max"].instant.strftime(
                    "%d/%m/%Y %H:%M:%S") + ") " + "\nMinimo: " + (
                    "{:.2f}".format(ssm["min"].value)) + " " + dta.unit + " (" + ssm["min"].instant.strftime(
                    "%d/%m/%Y %H:%M:%S") + ") " + dta.unit + "\nDeviazione Standard: " + (
                    "{:.2f}".format(ssm["stdev"])) + " " + dta.unit + "\nModa: " + (
                    "{:.2f}".format(ssm["mean"])) + " " + dta.unit + "\nNumero di rilevazioni: " + (
                    "{:.2f}".format(ssm["itemCount"])))
            i += 1
        return list
