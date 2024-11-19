from discord_webhook import DiscordWebhook, DiscordEmbed
from dotenv import load_dotenv
import os
import paramiko
import time
import re
from datetime import datetime, timezone


def run_ssh_command(hostname, port, username, password, command):
    # 創建一個 SSH 客戶端
    ssh_client = paramiko.SSHClient()
    # 自動加載主機的 SSH 密鑰
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # 連接到遠程主機
        ssh_client.connect(hostname, port=port, username=username, password=password)

        # 執行指令
        stdin, stdout, stderr = ssh_client.exec_command(command)
        return stdout.read().decode(), stderr.read().decode()

    except Exception as e:
        print(f"連接失敗: {e}")

    finally:
        ssh_client.close()


def send_discord_notification(url, embed: DiscordEmbed):
    webhook = DiscordWebhook(url=url)
    webhook.add_embed(embed)
    response = webhook.execute()
    if response.status_code == 200:
        print(f"已通知: {url}")
    else:
        print(f"通知失敗: {response.status_code}")


def is_valid_hpc_status(message):
    # 手動分段檢查每個部分，並考慮不同可能的空白符處理
    lines = [line.strip() for line in message.strip().split("\n") if line.strip()]

    # 檢查每個部分
    if lines[0] != "===== NYCU HPC Status =====":
        return False

    if lines[1] != "[ Last Update ]":
        return False

    if not re.match(r"Time: \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", lines[2]):
        return False

    if lines[3] != "[ Jobs Pending/Running ]":
        return False

    if not re.match(r"Count: \d+/\d+", lines[4]):
        return False

    if lines[5] != "[ CPU Cores Used/Total ]":
        return False

    if not all(re.match(r"DGX-CN\d+: \d+/\d+", line) for line in lines[6:8]):
        return False

    if lines[8] != "[ GPU Used/Total ]":
        return False

    if not all(re.match(r"DGX-CN\d+: \d+/\d+", line) for line in lines[9:11]):
        return False

    # 所有檢查都通過，則格式正確
    return True


def parse_status_message(message):
    # 提取時間
    time_match = re.search(r"Time: (.+)", message)
    last_update = time_match.group(1) if time_match else ""

    # 提取 Jobs 資訊
    jobs_match = re.search(r"Count: (\d+)/(\d+)", message)
    jobs_pending = int(jobs_match.group(1)) if jobs_match else 0
    jobs_running = int(jobs_match.group(2)) if jobs_match else 0

    # 提取 CPU Cores 資訊
    cpu_cores = {}
    cpu_section = message.split("[ CPU Cores Used/Total ]")[1].split(
        "[ GPU Used/Total ]"
    )[0]
    cpu_matches = re.findall(r"(DGX-CN\d+): (\d+)/(\d+)", cpu_section)
    for node, used, total in cpu_matches:
        cpu_cores[node] = {"Used": int(used), "Total": int(total)}

    # 提取 GPU 資訊
    gpu = {}
    gpu_section = message.split("[ GPU Used/Total ]")[1]
    gpu_matches = re.findall(r"(DGX-CN\d+): (\d+)/(\d+)", gpu_section)
    for node, used, total in gpu_matches:
        gpu[node] = {"Used": int(used), "Total": int(total)}

    # 組裝成 JSON 結構
    status_json = {
        "Last_Update": last_update,
        "Jobs": {"Pending": jobs_pending, "Running": jobs_running},
        "CPU_Cores": cpu_cores,
        "GPU": gpu,
    }

    return status_json


def compare_status_json(json1, json2, ignore_fields=None):
    # 移除 Last_Update 欄位進行比較
    json1_copy = json1.copy()
    json2_copy = json2.copy()
    for field in ignore_fields or []:
        json1_copy.pop(field, None)
        json2_copy.pop(field, None)

    # 比較兩個 JSON 是否相同
    return json1_copy == json2_copy


def get_status_embed(status_json, last_status_json=None, title=None, job_footer=True):
    last_update = datetime.strptime(
        status_json["Last_Update"], "%Y-%m-%d %H:%M:%S"
    ).astimezone(timezone.utc)

    cpu_description = ""
    for node, cores in status_json["CPU_Cores"].items():
        cpu_description += f"[{node}]: "
        if last_status_json and node in last_status_json["CPU_Cores"]:
            last_cores = last_status_json["CPU_Cores"][node]
            if cores["Used"] != last_cores["Used"]:
                cpu_description += f"**__{cores['Used']}__/{cores['Total']}**\n"
            else:
                cpu_description += f"**{cores['Used']}/{cores['Total']}**\n"
        else:
            cpu_description += f"**{cores['Used']}/{cores['Total']}**\n"

    gpu_description = ""
    for node, gpu in status_json["GPU"].items():
        gpu_description += f"[{node}]: "
        if last_status_json and node in last_status_json["GPU"]:
            last_gpu = last_status_json["GPU"][node]
            if gpu["Used"] != last_gpu["Used"]:
                gpu_description += f"**__{gpu['Used']}__/{gpu['Total']}**\n"
            else:
                gpu_description += f"**{gpu['Used']}/{gpu['Total']}**\n"
        else:
            gpu_description += f"**{gpu['Used']}/{gpu['Total']}**\n"

    job_discription = "[Jobs Pending/Running]: "
    if (
        last_status_json
        and last_status_json["Jobs"]["Pending"] != status_json["Jobs"]["Pending"]
    ):
        job_discription += (
            f"**__{status_json['Jobs']['Pending']}__**/{status_json['Jobs']['Running']}"
        )
    else:
        job_discription += (
            f"**{status_json['Jobs']['Pending']}**/{status_json['Jobs']['Running']}"
        )
    footer = f"[Jobs Pending/Running]: {status_json['Jobs']['Pending']}/{status_json['Jobs']['Running']}"

    embed = DiscordEmbed(title=title, color=242424)
    if job_footer:
        embed.set_footer(text=footer)
    else:
        embed.add_embed_field(name="", value=job_discription, inline=False)
    embed.add_embed_field(name="GPU Used/Total", value=gpu_description)
    embed.add_embed_field(name="CPU Used/Total", value=cpu_description)
    embed.set_timestamp(last_update)
    return embed


def h100_pooling(
    discord_full_monitor_webhook_url,
    discord_gpu_monitor_webhook_url,
    discord_zero_job_monitor_webhook_url,
    hostname,
    port,
    username,
    password,
    interval=60,
):
    last_status_json = None
    while True:
        current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        try:
            # get current status
            out, err = run_ssh_command(
                hostname=hostname,
                port=port,
                username=username,
                password=password,
                command="hpcs",
            )

            # is status valid
            if not is_valid_hpc_status(out):
                print(f"{current_time}: 無效的 HPC 狀態:\n{out}")
                continue
            current_status_json = parse_status_message(out)
            print(f"{current_time}: 狀態正常, 目前狀態:\n{out}")

            # first time request
            if last_status_json is None:
                print(f"{current_time}: 首次請求, 通知中...")
                last_status_json = current_status_json
                status_embed = get_status_embed(
                    current_status_json, title="HPC Initial Status"
                )
                job_status_embed = get_status_embed(
                    current_status_json,
                    last_status_json=last_status_json,
                    title="HPC Initial Status",
                    job_footer=False,
                )
                send_discord_notification(
                    discord_full_monitor_webhook_url, status_embed
                )
                send_discord_notification(discord_gpu_monitor_webhook_url, status_embed)
                send_discord_notification(
                    discord_zero_job_monitor_webhook_url, job_status_embed
                )

            # get status embed
            status_embed = get_status_embed(
                current_status_json, last_status_json=last_status_json
            )

            # compare status (full monitor)
            if compare_status_json(
                last_status_json, current_status_json, ignore_fields=["Last_Update"]
            ):
                print(f"{current_time} (full): 狀態未改變")
            else:
                print(f"{current_time} (full): 狀態已改變, 通知中...")
                send_discord_notification(
                    discord_full_monitor_webhook_url, status_embed
                )

            # compare status (gpu monitor)
            if compare_status_json(
                last_status_json,
                current_status_json,
                ignore_fields=["Last_Update", "Jobs", "CPU_Cores"],
            ):
                print(f"{current_time} (gpu): 狀態未改變")
            else:
                print(f"{current_time} (gpu): 狀態已改變, 通知中...")
                send_discord_notification(discord_gpu_monitor_webhook_url, status_embed)

            # compare status (zero job monitor)
            if (
                last_status_json["Jobs"]["Pending"] == 0
                and current_status_json["Jobs"]["Pending"] > 0
            ) or (
                last_status_json["Jobs"]["Pending"] > 0
                and current_status_json["Jobs"]["Pending"] == 0
            ):
                print(f"{current_time} (zero job): 狀態已改變, 通知中...")
                job_status_embed = get_status_embed(
                    current_status_json,
                    last_status_json=last_status_json,
                    job_footer=False,
                )
                send_discord_notification(
                    discord_zero_job_monitor_webhook_url, job_status_embed
                )
            else:
                print(f"{current_time} (zero job): 狀態未改變")

            # update last status
            last_status_json = current_status_json

            if err:
                print(f"{current_time}: 警告: {err}")

        except Exception as e:
            print(f"{current_time}: 請求失敗: {e}")
            if last_status_json is not None:
                print("通知中...")
                last_status_json = None
                send_discord_notification(
                    discord_full_monitor_webhook_url,
                    DiscordEmbed(
                        title="Cannot Fetch HPC Status", color=15158332
                    ),  # red
                )

        # 間隔一段時間再重新請求
        time.sleep(interval)


if __name__ == "__main__":
    load_dotenv()
    discord_full_monitor_webhook_url = os.getenv("DISCORD_FULL_MONITOR_WEBHOOK_URL")
    discord_gpu_monitor_webhook_url = os.getenv("DISCORD_GPU_MONITOR_WEBHOOK_URL")
    discord_zero_job_monitor_webhook_url = os.getenv(
        "DISCORD_ZERO_JOB_MONITOR_WEBHOOK_URL"
    )
    hostname = os.getenv("SSH_HOST")
    port = int(os.getenv("SSH_PORT"))
    username = os.getenv("SSH_USER")
    password = os.getenv("SSH_PASS")
    h100_pooling(
        discord_full_monitor_webhook_url,
        discord_gpu_monitor_webhook_url,
        discord_zero_job_monitor_webhook_url,
        hostname,
        port,
        username,
        password,
    )

"""
example json:
{
    "Last_Update": "2024-11-10 12:09:00",
    "Jobs": {
        "Pending": 10,
        "Running": 7
    },
    "CPU_Cores": {
        "DGX-CN01": {
            "Used": 0,
            "Total": 224
        },
        "DGX-CN02": {
            "Used": 132,
            "Total": 224
        }
    },
    "GPU": {
        "DGX-CN01": {
            "Used": 0,
            "Total": 8
        },
        "DGX-CN02": {
            "Used": 8,
            "Total": 8
        }
    }
}
"""

"""
example message:
===== NYCU HPC Status =====

[ Last Update ]
Time: 2024-11-10 12:08:07

[ Jobs Pending/Running ]
Count: 9/7

[ CPU Cores Used/Total ]
DGX-CN01: 0/224
DGX-CN02: 132/224

[ GPU Used/Total ]
DGX-CN01: 0/8
DGX-CN02: 8/8
"""
