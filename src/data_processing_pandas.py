from pathlib import Path
from pandas import concat, read_excel

from constants import IMPORT_PATH, EXPORT_DAILY_FILE_HEADER


def get_time_kwh_from_export_path(file_pattern):
    dir_path = Path(IMPORT_PATH)
    return concat(
        read_excel(
            file_name,
            header=3,
            names=EXPORT_DAILY_FILE_HEADER
        ) for file_name in dir_path.rglob(file_pattern)
    )


def refine_xls(file_pattern):
    input_df = get_time_kwh_from_export_path(f"*{file_pattern}*")
    input_df.fillna('', inplace=True)
    input_df.rename(columns={
        "Time":"Time",
        "InverterSN":"InverterSN",
        "Data LoggerSN":"DataLoggerSN",
        "Alert Details":"AlertDetails",
        "Alert Code":"AlertCode",
        "DC Voltage PV1(V)":"DCVoltagePV1V",
        "DC Voltage PV2(V)":"DCVoltagePV2V",
        "DC Current1(A)":"DCCurrent1A",
        "DC Current2(A)":"DCCurrent2A",
        "AC Voltage R/U/A(V)":"ACVoltageR_U_AV",
        "AC Voltage S/V/B(V)":"ACVoltageS_V_BV",
        "AC Voltage T/W/C(V)":"ACVoltageT_W_CV",
        "AC Current R/U/A(A)":"ACCurrentR_U_AA",
        "AC Current S/V/B(A)":"ACCurrentS_V_BA",
        "AC Current T/W/C(A)":"ACCurrentT_W_CA",
        "AC Output Total Power (Active)(W)":"ACOutputTotalPowerActiveW",
        "AC Output Frequency R(Hz)":"ACOutputFrequencyRHz",
        "Generation of Last Month (Active)(kWh)":"GenerationofLastMonthActive_kWh",
        "Daily Generation (Active)(kWh)":"DailyGenerationActive_kWh",
        "Total Generation (Active)(kWh)":"TotalGenerationActive_kWh",
        "Power Grid Total Power(W)":"PowerGridTotalPowerW",
        "Power Grid Total Reactive Power(Var)":"PowerGridTotalReactivePowerVar",
        "Power Grid Total Apparent Power(VA)":"PowerGridTotalApparentPowerVA",
        "Grid Power Factor":"GridPowerFactor",
        "Inverter Temperature(℃)":"InverterTemperature",
        "Power Limitation Percentage":"PowerLimitationPercentage",
        "Power On/Off Status":"PowerOnOffStatus"
    }, inplace=True)
    return input_df
