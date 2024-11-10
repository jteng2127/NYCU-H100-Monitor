from discord_webhook import DiscordWebhook, DiscordEmbed
from dotenv import load_dotenv
import os
import paramiko
import time
import re


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
        print(f"連接失敗：{e}")

    finally:
        ssh_client.close()


def send_discord_notification(url, title, link, description):
    webhook = DiscordWebhook(url=url)
    embed = DiscordEmbed(title=title, description=description, color=242424)
    webhook.add_embed(embed)
    response = webhook.execute()
    if response.status_code == 200:
        print(f"已通知：{title}")
    else:
        print(f"通知失敗：{response.status_code}")


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


def compare_status_json(json1, json2):
    # 移除 Last_Update 欄位進行比較
    json1_copy = {key: value for key, value in json1.items() if key != "Last_Update"}
    json2_copy = {key: value for key, value in json2.items() if key != "Last_Update"}

    # 比較兩個 JSON 是否相同
    return json1_copy == json2_copy


def h100_pooling(discord_webhook_url, hostname, port, username, password, interval=60):
    last_status_json = None
    while True:
        try:
            current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            out, err = run_ssh_command(
                hostname=hostname,
                port=port,
                username=username,
                password=password,
                command="hpcs",
            )

            if not is_valid_hpc_status(out):
                print(f"{current_time}：無效的 HPC 狀態:\n{out}")
                continue
            current_status_json = parse_status_message(out)
            if compare_status_json(last_status_json, current_status_json):
                print(f"{current_time}：狀態未改變")
            else:
                print(f"{current_time}：狀態已改變, 通知中...")
                last_status_json = current_status_json
                send_discord_notification(discord_webhook_url, "NYCU HPC 狀態更新", "", out)
            
        except Exception as e:
            print(f"請求失敗：{e}")

        # 間隔一段時間再重新請求
        time.sleep(interval)


if __name__ == "__main__":
    load_dotenv()
    discord_webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    hostname = os.getenv("SSH_HOST")
    port = int(os.getenv("SSH_PORT"))
    username = os.getenv("SSH_USER")
    password = os.getenv("SSH_PASS")
    h100_pooling(discord_webhook_url, hostname, port, username, password)

#     message = """
#     ===== NYCU HPC Status =====

# [ Last Update ]
# Time: 2024-11-10 12:08:07

# [ Jobs Pending/Running ]
# Count: 9/7

# [ CPU Cores Used/Total ]
# DGX-CN01: 0/224
# DGX-CN02: 132/224

# [ GPU Used/Total ]
# DGX-CN01: 0/8
# DGX-CN02: 8/8
# """
#     print(is_valid_hpc_status(message))
