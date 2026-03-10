import asyncio
import atexit
import os
import random
import sqlite3
import time
import sys
from datetime import datetime, timedelta
import fcntl

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN", "")
GUILD_ID = int(os.getenv("DISCORD_GUILD_ID", "0") or 0)
try:
    import imageio_ffmpeg
    _bundled_ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
except Exception:
    _bundled_ffmpeg = "ffmpeg"
FFMPEG_PATH = os.getenv("FFMPEG_PATH", _bundled_ffmpeg)
XP_COOLDOWN_SECONDS = 60
SWEAR_STRIKE_LIMIT = int(os.getenv("SWEAR_STRIKE_LIMIT", "3"))
MUTE_TIMEOUT_MINUTES = int(os.getenv("MUTE_TIMEOUT_MINUTES", "10"))
REPEAT_MUTE_TIMEOUT_MINUTES = int(os.getenv("REPEAT_MUTE_TIMEOUT_MINUTES", "30"))
LEVEL_DEATH_SECONDS = int(os.getenv("LEVEL_DEATH_SECONDS", str(19 * 24 * 60 * 60)))
STARTING_MONEY = int(os.getenv("STARTING_MONEY", "100"))
JOB_COOLDOWN_SECONDS = int(os.getenv("JOB_COOLDOWN_SECONDS", "30"))

BAD_WORDS = {
    w.strip().lower()
    for w in os.getenv("BAD_WORDS", "fuck,shit,bitch,asshole,gay,epstein,diddy,rape").split(",")
    if w.strip()
}
BYPASS_ROLE_NAMES = {
    r.strip().lower()
    for r in os.getenv("MODERATION_BYPASS_ROLES", "admin,administrator,mod,moderator").split(",")
    if r.strip()
}

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)


LOCK_FILE_PATH = "/Users/kevinkhoi/PycharmProjects/PythonProject/bot.lock"
_lock_file_handle = None


def acquire_single_instance_lock() -> bool:
    global _lock_file_handle
    _lock_file_handle = open(LOCK_FILE_PATH, "w")
    try:
        fcntl.flock(_lock_file_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        _lock_file_handle.write(str(os.getpid()))
        _lock_file_handle.flush()
        return True
    except OSError:
        return False


def release_single_instance_lock() -> None:
    global _lock_file_handle
    if _lock_file_handle is not None:
        try:
            fcntl.flock(_lock_file_handle.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        _lock_file_handle.close()
        _lock_file_handle = None

conn = sqlite3.connect("levels.db")
cursor = conn.cursor()

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS users (
        guild_id INTEGER,
        user_id INTEGER,
        xp INTEGER DEFAULT 0,
        level INTEGER DEFAULT 0,
        last_xp_time REAL DEFAULT 0,
        PRIMARY KEY (guild_id, user_id)
    )
    """
)
conn.commit()

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS server_stats_daily (
        guild_id INTEGER,
        day TEXT,
        messages INTEGER DEFAULT 0,
        joins INTEGER DEFAULT 0,
        PRIMARY KEY (guild_id, day)
    )
    """
)
conn.commit()

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS economy (
        guild_id INTEGER,
        user_id INTEGER,
        balance INTEGER DEFAULT 100,
        last_job_time REAL DEFAULT 0,
        PRIMARY KEY (guild_id, user_id)
    )
    """
)
conn.commit()

last_filter_permission_warn_at = 0.0
swear_strikes = {}
swear_mute_cycles = {}
last_xp_change_at = {}
last_known_total_xp = {}
last_death_notice_at = {}
last_seen_channel = {}
death_task = None
active_blackjack_games = set()

def xp_for_next_level(level: int) -> int:
    return 100 * (level + 1)


def level_xp_to_total_xp(level: int, xp: int) -> int:
    total = xp
    for lv in range(level):
        total += xp_for_next_level(lv)
    return total


def total_xp_to_level_xp(total_xp: int):
    level = 0
    xp = total_xp
    while xp >= xp_for_next_level(level):
        xp -= xp_for_next_level(level)
        level += 1
    return level, xp


def get_user_data(guild_id: int, user_id: int):
    cursor.execute(
        "SELECT xp, level, last_xp_time FROM users WHERE guild_id = ? AND user_id = ?",
        (guild_id, user_id),
    )
    row = cursor.fetchone()
    if row is None:
        cursor.execute(
            "INSERT INTO users (guild_id, user_id, xp, level, last_xp_time) VALUES (?, ?, 0, 0, 0)",
            (guild_id, user_id),
        )
        conn.commit()
        return 0, 0, 0
    return row


def update_user_data(guild_id: int, user_id: int, xp: int, level: int, last_xp_time: float):
    cursor.execute(
        """
        UPDATE users
        SET xp = ?, level = ?, last_xp_time = ?
        WHERE guild_id = ? AND user_id = ?
        """,
        (xp, level, last_xp_time, guild_id, user_id),
    )
    conn.commit()


def get_economy_data(guild_id: int, user_id: int):
    cursor.execute(
        "SELECT balance, last_job_time FROM economy WHERE guild_id = ? AND user_id = ?",
        (guild_id, user_id),
    )
    row = cursor.fetchone()
    if row is None:
        cursor.execute(
            "INSERT INTO economy (guild_id, user_id, balance, last_job_time) VALUES (?, ?, ?, 0)",
            (guild_id, user_id, STARTING_MONEY),
        )
        conn.commit()
        return STARTING_MONEY, 0
    return row


def update_economy_data(guild_id: int, user_id: int, balance: int, last_job_time: float):
    cursor.execute(
        """
        UPDATE economy
        SET balance = ?, last_job_time = ?
        WHERE guild_id = ? AND user_id = ?
        """,
        (balance, last_job_time, guild_id, user_id),
    )
    conn.commit()


def stats_day_key() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def increment_server_stats(guild_id: int, messages_inc: int = 0, joins_inc: int = 0):
    day = stats_day_key()
    cursor.execute(
        """
        INSERT INTO server_stats_daily (guild_id, day, messages, joins)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(guild_id, day)
        DO UPDATE SET
            messages = messages + excluded.messages,
            joins = joins + excluded.joins
        """,
        (guild_id, day, messages_inc, joins_inc),
    )
    conn.commit()


def get_server_stats(guild_id: int, period: str):
    today = datetime.utcnow().date()
    if period == "daily":
        day = today.strftime("%Y-%m-%d")
        cursor.execute(
            "SELECT COALESCE(messages, 0), COALESCE(joins, 0) FROM server_stats_daily WHERE guild_id = ? AND day = ?",
            (guild_id, day),
        )
    elif period == "monthly":
        start = today.replace(day=1).strftime("%Y-%m-%d")
        end = today.strftime("%Y-%m-%d")
        cursor.execute(
            """
            SELECT COALESCE(SUM(messages), 0), COALESCE(SUM(joins), 0)
            FROM server_stats_daily
            WHERE guild_id = ? AND day >= ? AND day <= ?
            """,
            (guild_id, start, end),
        )
    else:
        start = today.replace(month=1, day=1).strftime("%Y-%m-%d")
        end = today.strftime("%Y-%m-%d")
        cursor.execute(
            """
            SELECT COALESCE(SUM(messages), 0), COALESCE(SUM(joins), 0)
            FROM server_stats_daily
            WHERE guild_id = ? AND day >= ? AND day <= ?
            """,
            (guild_id, start, end),
        )

    row = cursor.fetchone()
    if not row:
        return 0, 0
    return row[0], row[1]




def member_can_bypass_filter(member: discord.Member) -> bool:
    if member.guild_permissions.manage_messages or member.guild_permissions.administrator:
        return True
    role_names = {role.name.lower() for role in member.roles}
    return bool(role_names & BYPASS_ROLE_NAMES)


def contains_bad_word(text: str) -> bool:
    lowered = text.lower()
    return any(bad in lowered for bad in BAD_WORDS)


def is_url(text: str) -> bool:
    return text.startswith("http://") or text.startswith("https://")


async def resolve_audio_source(query: str):
    try:
        import yt_dlp
    except Exception:
        raise RuntimeError("Missing dependency `yt-dlp`. Install with: pip install yt-dlp")

    ytdl_opts = {
        "format": "bestaudio/best",
        "noplaylist": True,
        "quiet": True,
        "default_search": "ytsearch",
        "extractor_args": {
            "youtube": {
                "player_client": ["android", "web"],
            }
        },
    }

    def _extract():
        with yt_dlp.YoutubeDL(ytdl_opts) as ydl:
            target = query if is_url(query) else f"ytsearch1:{query}"
            info = ydl.extract_info(target, download=False)
            if "entries" in info:
                info = info["entries"][0]
            return {
                "title": info.get("title", "Unknown Title"),
                "stream_url": info["url"],
                "webpage_url": info.get("webpage_url", query),
            }

    return await asyncio.to_thread(_extract)


BLACKJACK_SUITS = ["♠", "♥", "♦", "♣"]
BLACKJACK_RANKS = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"]


def blackjack_draw_card():
    rank = random.choice(BLACKJACK_RANKS)
    suit = random.choice(BLACKJACK_SUITS)
    return rank, suit


def blackjack_hand_value(hand):
    value = 0
    aces = 0
    for rank, _ in hand:
        if rank in ["J", "Q", "K"]:
            value += 10
        elif rank == "A":
            value += 11
            aces += 1
        else:
            value += int(rank)

    while value > 21 and aces > 0:
        value -= 10
        aces -= 1
    return value


def blackjack_hand_text(hand):
    return " ".join([f"{rank}{suit}" for rank, suit in hand])


class BlackjackView(discord.ui.View):
    def __init__(self, player_id: int, game_key):
        super().__init__(timeout=120)
        self.player_id = player_id
        self.game_key = game_key
        self.player_hand = [blackjack_draw_card(), blackjack_draw_card()]
        self.dealer_hand = [blackjack_draw_card(), blackjack_draw_card()]
        self.finished = False

    def build_embed(self, reveal_dealer=False, result_text=None):
        player_value = blackjack_hand_value(self.player_hand)
        dealer_value = blackjack_hand_value(self.dealer_hand)
        dealer_text = blackjack_hand_text(self.dealer_hand) if reveal_dealer else f"{self.dealer_hand[0][0]}{self.dealer_hand[0][1]} ??"
        dealer_display = str(dealer_value) if reveal_dealer else "?"

        embed = discord.Embed(title="Blackjack", color=discord.Color.dark_green())
        embed.add_field(name="Your Hand", value=f"{blackjack_hand_text(self.player_hand)}\nValue: **{player_value}**", inline=False)
        embed.add_field(name="Dealer Hand", value=f"{dealer_text}\nValue: **{dealer_display}**", inline=False)

        if result_text:
            embed.description = result_text
        else:
            embed.description = "Hit or Stand?"
        return embed

    async def finish_game(self, interaction: discord.Interaction, result_text: str):
        self.finished = True
        for item in self.children:
            item.disabled = True
        active_blackjack_games.discard(self.game_key)
        await interaction.response.edit_message(embed=self.build_embed(reveal_dealer=True, result_text=result_text), view=self)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.player_id:
            await interaction.response.send_message("This is not your blackjack game.", ephemeral=True)
            return False
        return True

    async def on_timeout(self):
        if self.finished:
            return
        self.finished = True
        for item in self.children:
            item.disabled = True
        active_blackjack_games.discard(self.game_key)

    @discord.ui.button(label="Hit", style=discord.ButtonStyle.primary)
    async def hit_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.finished:
            return

        self.player_hand.append(blackjack_draw_card())
        player_value = blackjack_hand_value(self.player_hand)

        if player_value > 21:
            await self.finish_game(interaction, "You busted. Dealer wins.")
            return

        await interaction.response.edit_message(embed=self.build_embed(reveal_dealer=False), view=self)

    @discord.ui.button(label="Stand", style=discord.ButtonStyle.secondary)
    async def stand_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.finished:
            return

        while blackjack_hand_value(self.dealer_hand) < 17:
            self.dealer_hand.append(blackjack_draw_card())

        player_value = blackjack_hand_value(self.player_hand)
        dealer_value = blackjack_hand_value(self.dealer_hand)

        if dealer_value > 21:
            result = "Dealer busted. You win."
        elif dealer_value > player_value:
            result = "Dealer wins."
        elif dealer_value < player_value:
            result = "You win."
        else:
            result = "Push (tie)."

        await self.finish_game(interaction, result)


class RussianRouletteView(discord.ui.View):
    def __init__(self, player_id: int, guild_id: int, bet: int, prize: int):
        super().__init__(timeout=120)
        self.player_id = player_id
        self.guild_id = guild_id
        self.bet = bet
        self.prize = prize
        self.rounds_survived = 1
        self.finished = False
        self.message = None

    def build_embed(self, text: str):
        embed = discord.Embed(title="Russian Roulette", description=text, color=discord.Color.dark_red())
        embed.add_field(name="Current Prize", value=f"**${self.prize}**", inline=True)
        embed.add_field(name="Rounds Survived", value=str(self.rounds_survived), inline=True)
        return embed

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.player_id:
            await interaction.response.send_message("This is not your russian roulette game.", ephemeral=True)
            return False
        return True

    async def finish(self):
        self.finished = True
        for item in self.children:
            item.disabled = True

    async def on_timeout(self):
        if self.finished:
            return
        await self.finish()
        if self.message is not None:
            try:
                await self.message.edit(embed=self.build_embed("Game timed out. No payout."), view=self)
            except discord.HTTPException:
                pass

    @discord.ui.button(label="Continue", style=discord.ButtonStyle.danger)
    async def continue_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.finished:
            return

        chamber = random.randint(1, 6)
        if chamber == 1:
            balance, last_job_time = get_economy_data(self.guild_id, self.player_id)
            update_economy_data(self.guild_id, self.player_id, 0, last_job_time)
            await self.finish()
            await interaction.response.edit_message(
                embed=self.build_embed(f"{interaction.user.mention} BANG. You lose all your money."),
                view=self,
            )
            return

        self.rounds_survived += 1
        self.prize += self.bet
        await interaction.response.edit_message(
            embed=self.build_embed("Blank chamber. Continue or cash out?"),
            view=self,
        )

    @discord.ui.button(label="Cash Out", style=discord.ButtonStyle.success)
    async def cashout_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.finished:
            return

        balance, last_job_time = get_economy_data(self.guild_id, self.player_id)
        new_balance = balance + self.prize
        update_economy_data(self.guild_id, self.player_id, new_balance, last_job_time)
        await self.finish()
        await interaction.response.edit_message(
            embed=self.build_embed(f"{interaction.user.mention} cashed out **${self.prize}**. New balance: **${new_balance}**"),
            view=self,
        )


class GiveawayView(discord.ui.View):
    def __init__(self, host_id: int, prize: str, winners_count: int, duration_seconds: int):
        super().__init__(timeout=duration_seconds)
        self.host_id = host_id
        self.prize = prize
        self.winners_count = winners_count
        self.duration_seconds = duration_seconds
        self.entries = set()
        self.message = None
        self.ended = False

    @discord.ui.button(label="Enter Giveaway", style=discord.ButtonStyle.success)
    async def enter_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.ended:
            await interaction.response.send_message("This giveaway already ended.", ephemeral=True)
            return
        if interaction.user.bot:
            await interaction.response.send_message("Bots cannot enter giveaways.", ephemeral=True)
            return

        if interaction.user.id in self.entries:
            await interaction.response.send_message("You are already entered.", ephemeral=True)
            return

        self.entries.add(interaction.user.id)
        await interaction.response.send_message("You are entered in the giveaway.", ephemeral=True)

    async def on_timeout(self):
        if self.ended:
            return
        self.ended = True
        for item in self.children:
            item.disabled = True

        if self.message is None:
            return

        if not self.entries:
            embed = discord.Embed(title="Giveaway Ended", description=f"Prize: **{self.prize}**\nNo valid entries.", color=discord.Color.red())
            try:
                await self.message.edit(embed=embed, view=self)
                await self.message.channel.send(f"<@{self.host_id}> giveaway ended with no entries.")
            except discord.HTTPException:
                pass
            return

        winner_count = min(self.winners_count, len(self.entries))
        winner_ids = random.sample(list(self.entries), k=winner_count)
        winner_mentions = ", ".join([f"<@{uid}>" for uid in winner_ids])

        embed = discord.Embed(
            title="Giveaway Ended",
            description=f"Prize: **{self.prize}**\nWinners: {winner_mentions}",
            color=discord.Color.gold(),
        )
        try:
            await self.message.edit(embed=embed, view=self)
            await self.message.channel.send(f"Congratulations {winner_mentions}! You won **{self.prize}**.")
        except discord.HTTPException:
            pass


async def get_ai_reply(user_text: str) -> str:
    system_prompt = (
        "You are a helpful Discord assistant. Reply naturally, keep answers concise, "
        "and format for chat readability."
    )

    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_text},
        ],
        "temperature": 0.7,
    }

    def _call_api():
        req = urllib.request.Request(
            "https://openrouter.ai/api/v1/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "HTTP-Referer": "https://discord.com",
                "X-Title": "Kogane Discord Bot",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=35) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data["choices"][0]["message"]["content"].strip()

    try:
        return await asyncio.to_thread(_call_api)
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="ignore")
        print("OpenRouter HTTPError:", e.code, detail)
        return "I hit an API error while replying."
    except Exception as e:
        print("OpenRouter error:", repr(e))
        return "I couldn't reply right now. Try again in a moment."


@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")
    if not getattr(bot, "tree_synced", False):
        # Use guild sync for instant slash-command updates.
        if GUILD_ID:
            guild_obj = discord.Object(id=GUILD_ID)
            try:
                bot.tree.clear_commands(guild=guild_obj)
                await bot.tree.sync(guild=guild_obj)
                bot.tree.copy_global_to(guild=guild_obj)
                synced = await bot.tree.sync(guild=guild_obj)
                print(f"Synced {len(synced)} guild slash command(s) for {GUILD_ID}: {[c.name for c in synced]}")
            except Exception as e:
                print(f"Guild sync failed for {GUILD_ID}: {e}")
        else:
            synced_global = await bot.tree.sync()
            print(f"Synced {len(synced_global)} global slash command(s): {[c.name for c in synced_global]}")

        bot.tree_synced = True

    global death_task
    if death_task is None or death_task.done():
        death_task = bot.loop.create_task(death_watcher_loop())


async def death_watcher_loop():
    while not bot.is_closed():
        now = time.time()
        for key, last_change in list(last_xp_change_at.items()):
            guild_id, user_id = key
            channel_id = last_seen_channel.get(key)
            if channel_id is None:
                continue

            last_notice = last_death_notice_at.get(key, 0)
            if now - last_change >= LEVEL_DEATH_SECONDS and now - last_notice >= LEVEL_DEATH_SECONDS:
                channel = bot.get_channel(channel_id)
                if channel is not None:
                    try:
                        await channel.send(f"<@{user_id}> you die in the culling games.")
                        last_death_notice_at[key] = now
                    except discord.HTTPException:
                        pass
        await asyncio.sleep(5)




@bot.event
async def on_member_join(member: discord.Member):
    increment_server_stats(member.guild.id, joins_inc=1)
    welcome_embed = discord.Embed()
    welcome_embed.set_image(url="https://media1.tenor.com/m/iOQRN_DKc7wAAAAd/kogane-jujutsu-kaisen.gif")
    try:
        if member.guild.system_channel and member.guild.system_channel.permissions_for(member.guild.me or member.guild.get_member(bot.user.id)).send_messages:
            await member.guild.system_channel.send(
                f"{member.mention} Welcome to the culling games",
                embed=welcome_embed,
            )
            return
    except Exception:
        pass

    # Fallback: first text channel where bot can send
    for channel in member.guild.text_channels:
        perms = channel.permissions_for(member.guild.me or member.guild.get_member(bot.user.id))
        if perms.send_messages:
            await channel.send(
                f"{member.mention} Welcome to the culling games",
                embed=welcome_embed,
            )
            break

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or message.guild is None:
        return

    guild_id = message.guild.id
    increment_server_stats(guild_id, messages_inc=1)
    user_id = message.author.id
    level_timer_key = (guild_id, user_id)
    message_text = message.content.strip()
    last_seen_channel[level_timer_key] = message.channel.id

    member = message.author if isinstance(message.author, discord.Member) else None
    if member and message_text and not member_can_bypass_filter(member):
        if contains_bad_word(message_text):
            global last_filter_permission_warn_at
            strike_key = (guild_id, user_id)
            swear_strikes[strike_key] = swear_strikes.get(strike_key, 0) + 1
            strikes = swear_strikes[strike_key]

            try:
                await message.delete()
                await message.channel.send(
                    f"{member.mention} watch your language. ({strikes}/{SWEAR_STRIKE_LIMIT})",
                    delete_after=4,
                )
            except discord.Forbidden:
                print("Filter delete failed: missing Manage Messages permission")
                if time.time() - last_filter_permission_warn_at > 60:
                    last_filter_permission_warn_at = time.time()
                    try:
                        await message.channel.send("I need `Manage Messages` permission to delete filtered words.")
                    except discord.HTTPException:
                        pass
            except discord.HTTPException as e:
                print(f"Filter delete failed HTTPException: {e}")

            if strikes >= SWEAR_STRIKE_LIMIT:
                try:
                    mute_key = (guild_id, user_id)
                    swear_mute_cycles[mute_key] = swear_mute_cycles.get(mute_key, 0) + 1
                    cycle_count = swear_mute_cycles[mute_key]
                    mute_minutes = MUTE_TIMEOUT_MINUTES if cycle_count == 1 else REPEAT_MUTE_TIMEOUT_MINUTES
                    await member.timeout(
                        timedelta(minutes=mute_minutes),
                        reason=f"{SWEAR_STRIKE_LIMIT} bad-word strikes",
                    )
                    swear_strikes[strike_key] = 0
                    banish_embed = discord.Embed()
                    banish_embed.set_image(url="https://media1.tenor.com/m/IZoBMTTAppEAAAAC/walter-white.gif")
                    await message.channel.send(
                        f"{member.mention} you been banished. Muted for {mute_minutes} minutes.",
                        embed=banish_embed,
                    )
                except discord.Forbidden:
                    await message.channel.send(
                        "I need `Moderate Members` permission to mute users.",
                        delete_after=6,
                    )
                except discord.HTTPException as e:
                    print(f"Mute failed HTTPException: {e}")
            return

    xp, level, last_xp_time = get_user_data(guild_id, user_id)
    total_xp_before = level_xp_to_total_xp(level, xp)

    now = time.time()
    if level_timer_key not in last_xp_change_at:
        last_xp_change_at[level_timer_key] = now
        last_known_total_xp[level_timer_key] = total_xp_before

    if now - last_xp_time >= XP_COOLDOWN_SECONDS:
        gained_xp = random.randint(10, 30)
        xp += gained_xp
        last_xp_time = now

        leveled_up = False
        while xp >= xp_for_next_level(level):
            xp -= xp_for_next_level(level)
            level += 1
            leveled_up = True

        update_user_data(guild_id, user_id, xp, level, last_xp_time)

        if leveled_up:
            await message.channel.send(f"{message.author.mention} leveled up to Level {level}!")

    total_xp_after = level_xp_to_total_xp(level, xp)
    if total_xp_after != last_known_total_xp.get(level_timer_key):
        last_known_total_xp[level_timer_key] = total_xp_after
        last_xp_change_at[level_timer_key] = now
        last_death_notice_at.pop(level_timer_key, None)

    last_notice = last_death_notice_at.get(level_timer_key, 0)
    if now - last_xp_change_at[level_timer_key] >= LEVEL_DEATH_SECONDS and now - last_notice >= LEVEL_DEATH_SECONDS:
        last_death_notice_at[level_timer_key] = now
        await message.channel.send(f"{message.author.mention} you die in the culling games.")

    await bot.process_commands(message)


@bot.tree.command(name="rank", description="Show your rank or another user's rank")
@app_commands.describe(member="User to check rank for")
async def rank_slash(interaction: discord.Interaction, member: discord.Member | None = None):
    if interaction.guild is None:
        await interaction.response.send_message("Use this command in a server.", ephemeral=True)
        return

    target = member or interaction.user
    xp, level, _ = get_user_data(interaction.guild.id, target.id)
    needed = xp_for_next_level(level)

    embed = discord.Embed(title=f"{target.display_name}'s Rank", color=discord.Color.blue())
    embed.add_field(name="Level", value=str(level), inline=True)
    embed.add_field(name="XP", value=f"{xp}/{needed}", inline=True)
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="leaderboard", description="Show top 10 XP users")
async def leaderboard_slash(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message("Use this command in a server.", ephemeral=True)
        return

    cursor.execute(
        """
        SELECT user_id, level, xp
        FROM users
        WHERE guild_id = ?
        ORDER BY level DESC, xp DESC
        LIMIT 10
        """,
        (interaction.guild.id,),
    )
    rows = cursor.fetchall()

    if not rows:
        await interaction.response.send_message("No leaderboard data yet.")
        return

    lines = []
    for i, (user_id, level, xp) in enumerate(rows, start=1):
        user = interaction.guild.get_member(user_id)
        name = user.display_name if user else f"User {user_id}"
        lines.append(f"**{i}.** {name} - Level {level} ({xp} XP)")

    embed = discord.Embed(
        title="Leaderboard",
        description="\n".join(lines),
        color=discord.Color.gold(),
    )
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="serverstats", description="Check server stats by period")
@app_commands.describe(period="daily, monthly, or yearly")
@app_commands.choices(
    period=[
        app_commands.Choice(name="daily", value="daily"),
        app_commands.Choice(name="monthly", value="monthly"),
        app_commands.Choice(name="yearly", value="yearly"),
    ]
)
async def serverstats_slash(interaction: discord.Interaction, period: app_commands.Choice[str]):
    if interaction.guild is None:
        await interaction.response.send_message("Use this command in a server.", ephemeral=True)
        return

    messages, joins = get_server_stats(interaction.guild.id, period.value)
    embed = discord.Embed(title="Server Stats", color=discord.Color.blurple())
    embed.add_field(name="Period", value=period.value.capitalize(), inline=True)
    embed.add_field(name="Messages", value=str(messages), inline=True)
    embed.add_field(name="New Members", value=str(joins), inline=True)
    embed.add_field(name="Current Members", value=str(interaction.guild.member_count or 0), inline=True)
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="transferxp", description="Transfer your XP to another user")
@app_commands.describe(member="User to receive XP", amount="XP amount to transfer")
async def transferxp_slash(interaction: discord.Interaction, member: discord.Member, amount: int):
    if interaction.guild is None:
        await interaction.response.send_message("Use this command in a server.", ephemeral=True)
        return

    if member.bot:
        await interaction.response.send_message("You can't transfer XP to a bot.", ephemeral=True)
        return

    if member.id == interaction.user.id:
        await interaction.response.send_message(
            "Use /givexp if you are a mod and want to grant XP.",
            ephemeral=True,
        )
        return

    if amount <= 0:
        await interaction.response.send_message("Amount must be more than 0.", ephemeral=True)
        return

    guild_id = interaction.guild.id
    sender_id = interaction.user.id
    receiver_id = member.id

    sender_xp, sender_level, sender_last = get_user_data(guild_id, sender_id)
    receiver_xp, receiver_level, receiver_last = get_user_data(guild_id, receiver_id)

    sender_total = level_xp_to_total_xp(sender_level, sender_xp)
    receiver_total = level_xp_to_total_xp(receiver_level, receiver_xp)

    if sender_total < amount:
        await interaction.response.send_message(
            f"You only have {sender_total} total XP available to transfer.",
            ephemeral=True,
        )
        return

    sender_total -= amount
    receiver_total += amount

    new_sender_level, new_sender_xp = total_xp_to_level_xp(sender_total)
    new_receiver_level, new_receiver_xp = total_xp_to_level_xp(receiver_total)
    leveled_up = new_receiver_level > receiver_level

    update_user_data(guild_id, sender_id, new_sender_xp, new_sender_level, sender_last)
    update_user_data(guild_id, receiver_id, new_receiver_xp, new_receiver_level, receiver_last)
    now = time.time()
    sender_key = (guild_id, sender_id)
    receiver_key = (guild_id, receiver_id)
    last_seen_channel[sender_key] = interaction.channel_id
    last_seen_channel[receiver_key] = interaction.channel_id
    last_known_total_xp[sender_key] = sender_total
    last_known_total_xp[receiver_key] = receiver_total
    last_xp_change_at[sender_key] = now
    last_xp_change_at[receiver_key] = now
    last_death_notice_at.pop(sender_key, None)
    last_death_notice_at.pop(receiver_key, None)

    msg = f"{interaction.user.mention} you have transfered **{amount} XP** to {member.mention}."
    if leveled_up:
        msg += f"\n{member.mention} is now **Level {new_receiver_level}**!"
    await interaction.response.send_message(msg)


@bot.tree.command(name="givexp", description="Give XP to a user (Manage Server only)")
@app_commands.describe(member="User to receive XP", amount="XP amount to give")
async def givexp_slash(interaction: discord.Interaction, member: discord.Member, amount: int):
    if interaction.guild is None:
        await interaction.response.send_message("Use this command in a server.", ephemeral=True)
        return

    if not isinstance(interaction.user, discord.Member) or not interaction.user.guild_permissions.manage_guild:
        await interaction.response.send_message(
            "You need the `Manage Server` permission to use this command.",
            ephemeral=True,
        )
        return

    if amount <= 0:
        await interaction.response.send_message("Amount must be more than 0.", ephemeral=True)
        return

    guild_id = interaction.guild.id
    user_xp, user_level, user_last = get_user_data(guild_id, member.id)

    user_xp += amount
    leveled_up = False
    while user_xp >= xp_for_next_level(user_level):
        user_xp -= xp_for_next_level(user_level)
        user_level += 1
        leveled_up = True

    update_user_data(guild_id, member.id, user_xp, user_level, user_last)
    now = time.time()
    target_key = (guild_id, member.id)
    last_seen_channel[target_key] = interaction.channel_id
    last_known_total_xp[target_key] = level_xp_to_total_xp(user_level, user_xp)
    last_xp_change_at[target_key] = now
    last_death_notice_at.pop(target_key, None)

    msg = f"You have given **{amount} XP** to {member.mention}."
    if member.id == interaction.user.id:
        msg = f"You have given yourself **{amount} XP**."
    if leveled_up:
        msg += f"\n{member.mention} is now **Level {user_level}**!"
    await interaction.response.send_message(msg)


@bot.tree.command(name="blackjack", description="Play blackjack against the dealer")
async def blackjack_slash(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message("Use this command in a server.", ephemeral=True)
        return

    game_key = (interaction.guild.id, interaction.channel_id, interaction.user.id)
    if game_key in active_blackjack_games:
        await interaction.response.send_message("You already have an active blackjack game in this channel.", ephemeral=True)
        return

    view = BlackjackView(player_id=interaction.user.id, game_key=game_key)
    active_blackjack_games.add(game_key)

    player_value = blackjack_hand_value(view.player_hand)
    dealer_value = blackjack_hand_value(view.dealer_hand)
    if player_value == 21 or dealer_value == 21:
        for item in view.children:
            item.disabled = True
        view.finished = True
        active_blackjack_games.discard(game_key)
        if player_value == 21 and dealer_value == 21:
            result = "Both hit blackjack. Push (tie)."
        elif player_value == 21:
            result = "Blackjack. You win."
        else:
            result = "Dealer has blackjack. Dealer wins."
        await interaction.response.send_message(embed=view.build_embed(reveal_dealer=True, result_text=result), view=view)
        return

    await interaction.response.send_message(embed=view.build_embed(reveal_dealer=False), view=view)


@bot.tree.command(name="balance", description="Check your money balance")
@app_commands.describe(member="User to check balance for")
async def balance_slash(interaction: discord.Interaction, member: discord.Member | None = None):
    if interaction.guild is None:
        await interaction.response.send_message("Use this command in a server.", ephemeral=True)
        return

    target = member or interaction.user
    balance, _ = get_economy_data(interaction.guild.id, target.id)
    await interaction.response.send_message(f"{target.mention} has **${balance}**.")


@bot.tree.command(name="job", description="Do a job to earn money")
async def job_slash(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message("Use this command in a server.", ephemeral=True)
        return

    guild_id = interaction.guild.id
    user_id = interaction.user.id
    balance, last_job_time = get_economy_data(guild_id, user_id)
    now = time.time()

    if now - last_job_time < JOB_COOLDOWN_SECONDS:
        remaining = int(JOB_COOLDOWN_SECONDS - (now - last_job_time))
        minutes, seconds = divmod(remaining, 60)
        await interaction.response.send_message(
            f"Job cooldown active. Try again in {minutes}m {seconds}s.",
            ephemeral=True,
        )
        return

    grade_rewards = [
        ("grade 4", 10),
        ("grade 3", 15),
        ("grade 2", 30),
        ("grade 1", 50),
        ("special grade", 300),
    ]
    random_jobs = [
        "patrolled Shibuya",
        "escorted civilians out of a barrier",
        "investigated a cursed warehouse",
        "guarded a colony checkpoint",
        "hunted cursed spirits near the colony",
        "tracked a cursed object in the subway",
        "sealed a minor cursed womb",
        "cleared a haunted school hallway",
        "scouted an abandoned hospital",
        "secured a cursed tool shipment",
        "swept a domain fragment for survivors",
        "guarded the jujutsu archive room",
        "repaired barrier talismans at dusk",
        "searched rooftops for curse signatures",
        "escorted a sorcerer trainee squad",
        "cleared a train platform infestation",
        "protected a ritual site overnight",
        "hunted a curse in storm drains",
        "recovered stolen talismans from a gang",
        "stabilized a collapsing barrier node",
    ]
    # 0.001% chance jackpot job.
    if random.random() < 0.00001:
        grade = "special grade"
        earned = 5000
        job_text = "defended a high-profile client"
    else:
        grade, earned = random.choice(grade_rewards)
        job_text = random.choice(random_jobs)
    balance += earned
    update_economy_data(guild_id, user_id, balance, now)

    await interaction.response.send_message(
        f"{interaction.user.mention} {job_text}, cleared **{grade}** curse spirit and earned **${earned}**. Balance: **${balance}**"
    )


@bot.tree.command(name="games", description="Play a game for money")
@app_commands.describe(
    game="Choose a game",
    bet="How much money to bet",
)
@app_commands.choices(
    game=[
        app_commands.Choice(name="blackjack", value="blackjack"),
        app_commands.Choice(name="wordle", value="wordle"),
        app_commands.Choice(name="chess", value="chess"),
        app_commands.Choice(name="roulette", value="roulette"),
        app_commands.Choice(name="slots", value="slots"),
        app_commands.Choice(name="russian roulette", value="russian_roulette"),
    ]
)
async def games_slash(interaction: discord.Interaction, game: app_commands.Choice[str], bet: int):
    if interaction.guild is None:
        await interaction.response.send_message("Use this command in a server.", ephemeral=True)
        return

    await interaction.response.defer(thinking=False)

    if bet <= 0:
        await interaction.followup.send("Bet must be more than 0.", ephemeral=True)
        return

    guild_id = interaction.guild.id
    user_id = interaction.user.id
    balance, last_job_time = get_economy_data(guild_id, user_id)

    if bet > balance:
        await interaction.followup.send(f"You only have **${balance}**.", ephemeral=True)
        return

    result_text = ""
    change = 0

    if game.value == "blackjack":
        player = random.randint(14, 23)
        dealer = random.randint(14, 23)
        if player > 21 and dealer > 21:
            result_text = f"Both busted (You: {player}, Dealer: {dealer}). Push."
            change = 0
        elif player > 21:
            result_text = f"You busted with {player}. Dealer wins."
            change = -bet
        elif dealer > 21 or player > dealer:
            result_text = f"You win (You: {player}, Dealer: {dealer})."
            change = bet
        elif dealer > player:
            result_text = f"Dealer wins (You: {player}, Dealer: {dealer})."
            change = -bet
        else:
            result_text = f"Push (You: {player}, Dealer: {dealer})."
            change = 0

    elif game.value == "wordle":
        win = random.random() < 0.40
        if win:
            change = int(bet * 1.5)
            result_text = f"You guessed the Wordle in time. You win **${change}**."
        else:
            change = -bet
            result_text = f"You ran out of guesses. You lose **${bet}**."

    elif game.value == "chess":
        roll = random.random()
        if roll < 0.45:
            change = bet
            result_text = f"You outplayed your opponent in chess. You win **${change}**."
        elif roll < 0.60:
            change = 0
            result_text = "Chess ended in a draw. Push."
        else:
            change = -bet
            result_text = f"You got checkmated. You lose **${bet}**."

    elif game.value == "roulette":
        wheel = random.randint(0, 36)
        if wheel == 0:
            change = bet * 4
            result_text = f"Roulette landed on **0**. Jackpot. You win **${change}**."
        elif wheel % 2 == 0:
            change = bet
            result_text = f"Roulette landed on **{wheel} (even)**. You win **${change}**."
        else:
            change = -bet
            result_text = f"Roulette landed on **{wheel} (odd)**. You lose **${bet}**."

    elif game.value == "slots":
        symbols = ["7", "BAR", "Cherry", "Bell", "Diamond", "Skull"]
        roll = [random.choice(symbols) for _ in range(3)]
        if roll[0] == roll[1] == roll[2]:
            multiplier = 5 if roll[0] != "Skull" else 8
            change = bet * multiplier
            result_text = f"Slots: {' | '.join(roll)}\nJACKPOT x{multiplier}. You win **${change}**."
        elif len(set(roll)) == 2:
            multiplier = 2
            change = bet * multiplier
            result_text = f"Slots: {' | '.join(roll)}\nPair hit x{multiplier}. You win **${change}**."
        else:
            change = -bet
            result_text = f"Slots: {' | '.join(roll)}\nNo match. You lose **${bet}**."

    elif game.value == "russian_roulette":
        lose = random.randint(1, 6) == 1
        if lose:
            change = -balance
            result_text = f"You lost russian roulette. You lose all your money (**${balance}**)."
        else:
            view = RussianRouletteView(
                player_id=interaction.user.id,
                guild_id=guild_id,
                bet=bet,
                prize=bet * 2,
            )
            view.message = await interaction.followup.send(
                embed=view.build_embed("Blank chamber. Continue or cash out?"),
                view=view,
                wait=True,
            )
            return

    balance += change
    if balance < 0:
        balance = 0
    update_economy_data(guild_id, user_id, balance, last_job_time)

    await interaction.followup.send(
        f"{interaction.user.mention} played **{game.value}** with **${bet}**.\n{result_text}\nBalance: **${balance}**"
    )


@bot.tree.command(name="giveaway", description="Start a giveaway with button entries")
@app_commands.describe(
    prize="What is the giveaway prize",
    duration_seconds="How long giveaway runs (seconds)",
    winners="Number of winners",
)
async def giveaway_slash(interaction: discord.Interaction, prize: str, duration_seconds: int = 60, winners: int = 1):
    if interaction.guild is None:
        await interaction.response.send_message("Use this command in a server.", ephemeral=True)
        return

    if duration_seconds < 10 or duration_seconds > 604800:
        await interaction.response.send_message("Duration must be between 10 and 604800 seconds.", ephemeral=True)
        return

    if winners < 1 or winners > 20:
        await interaction.response.send_message("Winners must be between 1 and 20.", ephemeral=True)
        return

    view = GiveawayView(
        host_id=interaction.user.id,
        prize=prize,
        winners_count=winners,
        duration_seconds=duration_seconds,
    )

    embed = discord.Embed(
        title="Giveaway Started",
        description=(
            f"Prize: **{prize}**\n"
            f"Host: {interaction.user.mention}\n"
            f"Winners: **{winners}**\n"
            f"Ends in: **{duration_seconds} seconds**\n\n"
            "Click **Enter Giveaway** to join."
        ),
        color=discord.Color.green(),
    )
    await interaction.response.send_message(embed=embed, view=view)
    view.message = await interaction.original_response()


async def _play_track(interaction: discord.Interaction, search_query: str):
    if interaction.guild is None:
        await interaction.response.send_message("Use this command in a server.", ephemeral=True)
        return

    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if member is None or member.voice is None or member.voice.channel is None:
        await interaction.response.send_message("Join a voice channel first.", ephemeral=True)
        return

    await interaction.response.defer(thinking=True)

    voice_channel = member.voice.channel
    voice_client = interaction.guild.voice_client

    try:
        if voice_client is None:
            voice_client = await voice_channel.connect()
        elif voice_client.channel != voice_channel:
            await voice_client.move_to(voice_channel)

        track = await resolve_audio_source(search_query)
        source = discord.FFmpegOpusAudio(
            executable=FFMPEG_PATH,
            source=track["stream_url"],
            before_options="-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5",
            options="-vn",
        )

        if voice_client.is_playing() or voice_client.is_paused():
            voice_client.stop()

        voice_client.play(source, after=lambda e: print(f"Audio player error: {e}") if e else None)
        await interaction.followup.send(f"Now playing: **{track['title']}**\n{track['webpage_url']}")
    except Exception as e:
        await interaction.followup.send(
            f"Could not play audio: {type(e).__name__}: {str(e) or repr(e)}",
            ephemeral=True,
        )


@bot.tree.command(name="play", description="Play music by song name and artist")
@app_commands.describe(name="Song name", artist="Artist name")
async def play_slash(interaction: discord.Interaction, name: str, artist: str):
    await _play_track(interaction, f"{name} {artist}")


@bot.tree.command(name="stop", description="Stop music playback")
async def stop_slash(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message("Use this command in a server.", ephemeral=True)
        return

    vc = interaction.guild.voice_client
    if vc is None or not vc.is_connected():
        await interaction.response.send_message("I am not in a voice channel.", ephemeral=True)
        return

    if vc.is_playing() or vc.is_paused():
        vc.stop()
        await interaction.response.send_message("Stopped playback.")
    else:
        await interaction.response.send_message("Nothing is playing.", ephemeral=True)


@bot.tree.command(name="leave", description="Disconnect bot from voice channel")
async def leave_slash(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message("Use this command in a server.", ephemeral=True)
        return

    vc = interaction.guild.voice_client
    if vc is None or not vc.is_connected():
        await interaction.response.send_message("I am not in a voice channel.", ephemeral=True)
        return

    await vc.disconnect()
    await interaction.response.send_message("Disconnected from voice channel.")


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    print(f"Slash command error: {error!r}")
    try:
        if interaction.response.is_done():
            await interaction.followup.send(f"Command failed: {error}", ephemeral=True)
        else:
            await interaction.response.send_message(f"Command failed: {error}", ephemeral=True)
    except discord.HTTPException:
        pass


if not TOKEN:
    raise RuntimeError("Set DISCORD_TOKEN in your .env file.")


if not acquire_single_instance_lock():
    print("Another bot instance is already running. Exiting this process.")
    sys.exit(0)
atexit.register(release_single_instance_lock)



bot.run(TOKEN)
