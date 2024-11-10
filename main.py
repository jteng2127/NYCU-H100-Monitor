from discord_webhook import DiscordWebhook, DiscordEmbed
from dotenv import load_dotenv
import os
import paramiko

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


def h100_pooling(discord_webhook_url, hostname, port, username, password):
    out, err = run_ssh_command(
        hostname=hostname,
        port=port,
        username=username,
        password=password,
        command="hpcs"
    )

    send_discord_notification(discord_webhook_url, "Changed", "", out)


if __name__ == "__main__":
    load_dotenv()
    discord_webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    hostname = os.getenv("SSH_HOST")
    port = int(os.getenv("SSH_PORT"))
    username = os.getenv("SSH_USER")
    password = os.getenv("SSH_PASS")
    h100_pooling(discord_webhook_url, hostname, port, username, password)
