# coding:utf-8
import datetime
import os
import re
import sys
import time

import numpy

from mongo_custom_client import Database

sys.path.append(os.pardir)


class LogFileProcessor:

    def __init__(self, file_path):
        self.file_path = file_path

    def parse(self):
        db = Database("127.0.0.1", 27017, "log_collection")

        if os.path.isfile(self.file_path) is False:
            # 这里需要时文件才能继续process
            raise Exception("Invalidate file path: " + self.file_path)

        access_delta_list = []
        tmp_access_delta_list = []

        core_delta_list = []
        tmp_core_delta_list = []
        alpha_count = 0
        gamma_count = 0

        with open(self.file_path, "r") as f:
            for line in f:
                if len(line.strip()) == 0:
                    print(">> Ignore Empty Line Data <<")
                    continue
                time_1 = re.findall(r'^.+, \\"pubTime\\":\\"(.+?)\\",.+', line.strip())[0]
                time_suffix = time_1[-6:-3]
                timeArray = time.strptime(time_1, "%Y-%m-%d %H:%M:%S.%f")
                time_1_after = int(time.mktime(timeArray) * 1000) + int(time_suffix)
                time_2 = re.findall(r'^.+} time (.+?)\[INFO.+', line.strip())[0]
                msg_id = re.findall(r'^.+\\"msgId\\":\\"(.+?)\\",.+', line.strip())[0]
                delta = int(time_2) - time_1_after

                model_data = {}
                model_data.update({
                    "msg_id": msg_id,
                    "time_1": time_1_after,
                    "time_2": int(time_2),
                    "delta": delta,
                    "duplicate": 1
                    })

                if "gamma publish request payload" in line:
                    # result = re.findall(r'^.+{"msgId":"(.+?)",.+', line.strip())
                    # 解析字段
                    # 转为json数据
                    # if model_data and model_data not in gamma_list:
                        # gamma_list.append(model_data)
                    tmp_access_delta_list.append(delta)
                    gamma_count += 1
                    # elif model_data in gamma_list:
                    #     model_data.update({
                    #         "duplicate": model_data.get("duplicate") + 1
                    #         })
                    # else:
                    #     continue

                elif "alpha publish request payload" in line:

                    # if model_data and model_data not in alpha_list:
                        # alpha_list.append(model_data)
                    tmp_core_delta_list.append(delta)
                    alpha_count += 1
                    # elif model_data in alpha_list:
                    #     model_data.update({
                    #         "duplicate": model_data.get("duplicate") + 1
                    #         })
                    # else:
                    #     continue
                else:
                    continue

                if alpha_count > 0 and alpha_count % 1000 == 0:
                    # db.insert_many("alpha_kafka_log_20210706", alpha_list)
                    # delta_model = {
                    #     "std": numpy.std(delta_list),  # 方差
                    #     "avg": numpy.mean(delta_list),  # 平均值
                    #     "median": numpy.median(delta_list),  # 中位数
                    #     "max": numpy.max(delta_list),
                    #     "min": numpy.min(delta_list)
                    #     }
                    # db.insert_one("alpha_delta_log_20210706", delta_model)
                    # delta_list = []
                    # alpha_list = []
                    core_delta_list.extend(tmp_core_delta_list)
                    tmp_core_delta_list = []
                    print("alpha_kafka_log_20210706 has processing ", alpha_count)
                #
                if gamma_count > 0 and gamma_count % 1000 == 0:
                    # db.insert_many("gamma_kafka_log_20210706", gamma_list)
                    #
                    # delta_model = {
                    #     "std": numpy.std(delta_list),  # 方差
                    #     "avg": numpy.mean(delta_list),  # 平均值
                    #     "median": numpy.median(delta_list),  # 中位数
                    #     "max": numpy.max(delta_list),
                    #     "min": numpy.min(delta_list)
                    #     }
                    # db.insert_one("gamma_delta_log_20210706", delta_model)
                    # gamma_list = []
                    # delta_list = []
                    access_delta_list.extend(tmp_access_delta_list)
                    tmp_access_delta_list = []
                    print("gamma_kafka_log_20210706 has processing ", gamma_count)

        if len(core_delta_list) > 0:
            delta_model = {
                "std": float(numpy.std(core_delta_list)),  # 方差
                "avg": float(numpy.mean(core_delta_list)),  # 平均值
                "median": float(numpy.median(core_delta_list)),  # 中位数
                "max": float(numpy.max(core_delta_list)),
                "min": float(numpy.min(core_delta_list))
                }
            db.insert_one("alpha_delta_log_20210706_145247", delta_model)
            print("alpha_kafka_log_20210706 processing will be end  , total commit ", len(core_delta_list))

        if len(access_delta_list) > 0:
            delta_model = {
                "std": float(numpy.std(access_delta_list)),  # 方差
                "avg": float(numpy.mean(access_delta_list)),  # 平均值
                "median": float(numpy.median(access_delta_list)),  # 中位数
                "max": float(numpy.max(access_delta_list)),
                "min": float(numpy.min(access_delta_list))
                }
            db.insert_one("gamma_delta_log_20210706_145247", delta_model)
            print("gamma_kafka_log_20210706 processing will be end  , total commit ", len(access_delta_list))


if __name__ == '__main__':
    pubfile = LogFileProcessor(
            # "/Users/eayonyu/Downloads/日志工具/control-service-merge-log-example.java"
            "/Users/eayonyu/Downloads/log_troubleshoot/kafka_pressure/145247/alpha_analysis_145247.log"
            )
    pubfile.parse()
    # line = '''2021-06-10 18:29:26.886 [INFO BEGIN] [KafkaConsumerDestination{consumerDestinationName='iot_access_subscribe_message', partitions=12, dlqName='null'}.container-0-C-1]:c.t.i.c.c.a.interfaces.listener.ReceiverListener - [,] - [receiveGamma:26]   gamma publish request payload {"origin":"access-subscribe-service","from":"/sys/u0Nzg5o9tgBavTfV/mqtt-benchmark-9/thing/event/electricitystatus/post","to":"/sys/u0Nzg5o9tgBavTfV/mqtt-benchmark-9/thing/event/electricitystatus/post","data":"{\"msgId\":\"p_20210610-182511-mqtt-benchmark-9-0\", \"pubTime\":\"2021-06-10 18:29:26.873154\", \"params\":{\"electricity\":{\"value\":0.1,\"time\":1623320966873154972},\"startTime\":{\"value\":0000000000,\"time\":0000000000},\"endTime\":{\"value\":0000000000,\"time\":0000000000}}, \"version\":\"1.0\"}","domain":"deviceDomain","messageEnum":"MESSAGE"} time 1623320966886   [INFO END]'''
    # line ='''2021-06-10 18:29:26.907 [INFO BEGIN] [KafkaConsumerDestination{consumerDestinationName='iot_polymerization_transmit_message_post', partitions=24, dlqName='null'}.container-0-C-1]:c.t.i.c.c.a.interfaces.listener.ReceiverListener - [,] - [receiveAlpha:16]   alpha publish request payload {|  "topic" : "/sys/u0Nzg5o9tgBavTfV/mqtt-benchmark-15/thing/event/electricitystatus/post",|  "data" : "{\"msgId\":\"p_20210610-182511-mqtt-benchmark-15-0\", \"pubTime\":\"2021-06-10 18:29:26.873845\", \"params\":{\"electricity\":{\"value\":0.1,\"time\":1623320966873845005},\"startTime\":{\"value\":0000000000,\"time\":0000000000},\"endTime\":{\"value\":0000000000,\"time\":0000000000}}, \"version\":\"1.0\"}"|} time 1623320966907   [INFO END]'''
    # line='''2021-06-10 18:29:26.894 [INFO BEGIN] [KafkaConsumerDestination{consumerDestinationName='iot_polymerization_transmit_message_post', partitions=24, dlqName='null'}.container-0-C-1]:c.t.i.c.c.a.interfaces.listener.ReceiverListener - [,] - [receiveAlpha:16]   alpha publish request payload {|  "topic" : "/sys/u0Nzg5o9tgBavTfV/mqtt-benchmark-6/thing/event/electricitystatus/post",|  "data" : "{\"msgId\":\"p_20210610-182511-mqtt-benchmark-6-0\", \"pubTime\":\"2021-06-10 18:29:26.872970\", \"params\":{\"electricity\":{\"value\":0.1,\"time\":1623320966872970574},\"startTime\":{\"value\":0000000000,\"time\":0000000000},\"endTime\":{\"value\":0000000000,\"time\":0000000000}}, \"version\":\"1.0\"}"|} time 1623320966894   [INFO END] '''
    #
    # time_1 = re.findall(r'^.+, \"pubTime\":\"(.+?)\",.+', line.strip())[0]
    # time_suffix = time_1[-6:-3]
    # timeArray = time.strptime(time_1, "%Y-%m-%d %H:%M:%S.%f")
    # timeStamp = int(time.mktime(timeArray) * 1000) + int(time_suffix)
    #
    # # time_1_after = time_1[:-6]
    # # time_1_after = round(int(time_1) / 1000000)
    # time_2 = re.findall(r'^.+} time (.+?)\[INFO.+', line.strip())[0]
    # msg_id = re.findall(r'^.+\"msgId\":\"(.+?)\",.+', line.strip())[0]
    # delta = int(time_2) - timeStamp
    #
    # print time_1, timeStamp, time_2, msg_id,  delta
