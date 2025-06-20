import re
import asyncio
import logging
import aiohttp
import signal
import sys
from datetime import datetime, timedelta
from pyrogram.enums import ParseMode
from pyrogram import Client, filters, idle
from pyrogram.errors import (
    UserAlreadyParticipant,
    InviteHashExpired,
    InviteHashInvalid,
    PeerIdInvalid,
    ChannelPrivate,
    UsernameNotOccupied,
    FloodWait,
    MessageTooLong,
    ChatWriteForbidden,
    UserBannedInChannel
)

# Enhanced logging setup
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('cc_scraper.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Configuration - UPDATED WITH MULTIPLE GROUPS
API_ID = "23925218"
API_HASH = "396fd3b1c29a427df8cc6fb54f3d307c"
PHONE_NUMBER = "+918123407093"

# MULTIPLE SOURCE GROUPS - ADD YOUR GROUPS HERE!
SOURCE_GROUPS = [
    -1002273285238,
    -1002682944548,
    -1002319403142,# Original group
    # Add your additional groups here:
    # -1002647815754,  # Example additional group
    # -1002647815755,  # Example additional group
]

# MULTIPLE TARGET CHANNELS - ADD YOUR CHANNELS HERE!
TARGET_CHANNELS = [
    -1002328456850, # Original channel
    # Add your additional channels here:
    # -1002783784145,  # Example additional channel
    # -1002783784146,  # Example additional channel
]

# ENHANCED SETTINGS WITH DELAYS
POLLING_INTERVAL = 3  # 3 seconds - More stable
MESSAGE_BATCH_SIZE = 50  # Smaller batch for stability
MAX_WORKERS = 50  # Reduced workers for stability
SEND_DELAY = 2  # 2 SECOND DELAY BETWEEN CARD SENDS!
PROCESS_DELAY = 0.3  # Small delay between processing
BIN_TIMEOUT = 10  # Timeout for bin lookup
MAX_CONCURRENT_CARDS = 25  # Reduced concurrency
MAX_PROCESSED_MESSAGES = 5000  # Reduced memory usage
RETRY_ATTEMPTS = 3  # Number of retry attempts for failed sends

# Enhanced client with better error handling
user = Client(
    "cc_monitor_user",
    api_id=API_ID,
    api_hash=API_HASH,
    phone_number=PHONE_NUMBER,
    workers=MAX_WORKERS,
    sleep_threshold=60  # Add sleep threshold for flood wait
)

# Global state
is_running = True
last_processed_message_ids = {}  # Track per group
processed_messages = set()
processed_cards = set()  # DUPLICATE PREVENTION!
accessible_channels = []  # Store accessible channels
stats = {
    'messages_processed': 0,
    'cards_found': 0,
    'cards_sent': 0,
    'cards_duplicated': 0,
    'send_failures': 0,
    'errors': 0,
    'start_time': None,
    'last_speed_check': None,
    'cards_per_second': 0,
    'bin_lookups_success': 0,
    'bin_lookups_failed': 0
}

# BIN Cache for permanent storage
bin_cache = {}

# Semaphore for controlled concurrent processing
card_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CARDS)

# New simplified BIN lookup function using the given URL
async def get_bin_info_antipublic(bin_number: str):
    """Lookup BIN info from https://bins.antipublic.cc/bins/{bin_code}"""
    if bin_number in bin_cache:
        logger.info(f"✅ BIN {bin_number} found in cache")
        return bin_cache[bin_number]

    url = f"https://bins.antipublic.cc/bins/{bin_number}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    try:
        timeout = aiohttp.ClientTimeout(total=BIN_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers, ssl=False) as response:
                if response.status == 200:
                    data = await response.json()

                    # Basic validation and normalization of response data
                    if data and isinstance(data, dict):
                        # Normalize keys if possible, fallback to UNKNOWN if missing
                        scheme = data.get('scheme', 'UNKNOWN')
                        card_type = data.get('type', 'UNKNOWN')
                        brand = data.get('brand', 'UNKNOWN')
                        bank = data.get('bank', 'UNKNOWN BANK')
                        country_name = data.get('country_name', 'UNKNOWN')
                        country_flag = data.get('country_flag', '🌍') if 'country_flag' in data else '🌍'

                        bin_info = {
                            'scheme': scheme.upper() if isinstance(scheme, str) else 'UNKNOWN',
                            'type': card_type.upper() if isinstance(card_type, str) else 'UNKNOWN',
                            'brand': brand.upper() if isinstance(brand, str) else 'UNKNOWN',
                            'bank': bank,
                            'country_name': country_name,
                            'country_flag': country_flag,
                        }

                        bin_cache[bin_number] = bin_info
                        stats['bin_lookups_success'] += 1
                        logger.info(f"✅ BIN {bin_number} lookup successful from antipublic")
                        return bin_info
                else:
                    logger.warning(f"⚠️ BIN lookup returned status {response.status} for {bin_number}")
    except Exception as e:
        logger.warning(f"❌ BIN lookup error for {bin_number}: {e}")

    stats['bin_lookups_failed'] += 1
    logger.warning(f"❌ BIN lookup failed for {bin_number}")
    return None


async def discover_accessible_channels():
    """Discover which channels are accessible and update the list"""
    global accessible_channels
    accessible_channels = []

    logger.info("🔍 Discovering accessible channels...")

    # First, try to get all dialogs to see what's available
    try:
        async for dialog in user.get_dialogs():
            if dialog.chat.type in ["channel", "supergroup"]:
                # Check if this is one of our target channels
                if dialog.chat.id in TARGET_CHANNELS:
                    logger.info(f"✅ Found target channel in dialogs: {dialog.chat.title} (ID: {dialog.chat.id})")
                    accessible_channels.append(dialog.chat.id)
                else:
                    logger.info(f"📢 Available channel: {dialog.chat.title} (ID: {dialog.chat.id})")
    except Exception as e:
        logger.error(f"❌ Error getting dialogs: {e}")

    # Try direct access to target channels
    for channel_id in TARGET_CHANNELS:
        if channel_id not in accessible_channels:
            try:
                chat = await user.get_chat(channel_id)
                logger.info(f"✅ Direct access to channel: {chat.title} (ID: {channel_id})")
                accessible_channels.append(channel_id)
            except PeerIdInvalid:
                logger.warning(f"⚠️ Channel {channel_id} not accessible (Peer ID Invalid)")
                logger.info(f"💡 Make sure the bot is added to channel {channel_id} as admin")
            except ChannelPrivate:
                logger.warning(f"⚠️ Channel {channel_id} is private")
            except Exception as e:
                logger.error(f"❌ Cannot access channel {channel_id}: {e}")

    if accessible_channels:
        logger.info(f"✅ Found {len(accessible_channels)} accessible channels: {accessible_channels}")
        return True
    else:
        logger.error("❌ No accessible channels found!")
        logger.info("💡 SOLUTION: Add your bot to the target channels as admin")
        logger.info("💡 Or use channel IDs from your available channels listed above")
        return False

async def test_channel_sending():
    """Test sending to accessible channels"""
    if not accessible_channels:
        logger.error("❌ No accessible channels to test")
        return False

    logger.info("🧪 Testing message sending to accessible channels...")

    for channel_id in accessible_channels:
        try:
            test_message = f"🔧 CC Scraper connection test - {datetime.now().strftime('%H:%M:%S')}"
            sent_msg = await user.send_message(
                chat_id=channel_id,
                text=test_message
            )
            logger.info(f"✅ Test message sent successfully to {channel_id}")

            # Delete test message after 3 seconds
            await asyncio.sleep(3)
            try:
                await user.delete_messages(channel_id, sent_msg.id)
                logger.info(f"✅ Test message deleted from {channel_id}")
            except:
                pass

        except Exception as e:
            logger.error(f"❌ Cannot send to channel {channel_id}: {e}")
            # Remove from accessible channels if sending fails
            if channel_id in accessible_channels:
                accessible_channels.remove(channel_id)

    if accessible_channels:
        logger.info(f"✅ {len(accessible_channels)} channels ready for sending")
        return True
    else:
        logger.error("❌ No channels available for sending")
        return False

async def send_to_target_channels_enhanced(formatted_message, cc_data):
    """Enhanced message sending with better error handling"""
    # Check for duplicates
    card_hash = cc_data.split('|')[0]  # Use card number as hash
    if card_hash in processed_cards:
        logger.info(f"🔄 DUPLICATE CC DETECTED: {cc_data[:12]}*** - SKIPPING")
        stats['cards_duplicated'] += 1
        return False

    # Add to processed cards
    processed_cards.add(card_hash)

    # Manage memory for processed cards
    if len(processed_cards) > 5000:
        processed_cards_list = list(processed_cards)
        processed_cards.clear()
        processed_cards.update(processed_cards_list[-2500:])

    if not accessible_channels:
        logger.error("❌ No accessible channels available for sending")
        stats['send_failures'] += 1
        return False

    success_count = 0

    # Send to each accessible channel
    for i, channel_id in enumerate(accessible_channels):
        for attempt in range(RETRY_ATTEMPTS):
            try:
                logger.info(f"📤 Sending CC {cc_data[:12]}*** to channel {channel_id} (attempt {attempt + 1})")

                await user.send_message(
                    chat_id=channel_id,
                    text=formatted_message,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True
                )

                logger.info(f"✅ SUCCESSFULLY SENT CC {cc_data[:12]}*** to channel {channel_id}")
                stats['cards_sent'] += 1
                success_count += 1
                break  # Success, no need to retry

            except FloodWait as e:
                logger.warning(f"⏳ Flood wait {e.value}s for channel {channel_id}")
                await asyncio.sleep(e.value + 1)
                continue  # Retry after flood wait

            except MessageTooLong:
                logger.error(f"❌ Message too long for channel {channel_id}")
                break  # Don't retry for this error

            except (ChatWriteForbidden, UserBannedInChannel):
                logger.error(f"❌ No permission to write to channel {channel_id}")
                # Remove from accessible channels
                if channel_id in accessible_channels:
                    accessible_channels.remove(channel_id)
                break  # Don't retry for permission errors

            except Exception as e:
                logger.error(f"❌ Failed to send CC to channel {channel_id} (attempt {attempt + 1}): {e}")
                if attempt == RETRY_ATTEMPTS - 1:  # Last attempt
                    stats['send_failures'] += 1
                else:
                    await asyncio.sleep(1)  # Wait before retry

        # Add delay between channels (except for last channel)
        if i < len(accessible_channels) - 1:
            logger.info(f"⏳ Waiting {SEND_DELAY} seconds before next channel...")
            await asyncio.sleep(SEND_DELAY)

    return success_count > 0

def extract_credit_cards_enhanced(text):
    """Enhanced credit card extraction"""
    if not text:
        return []

    # Enhanced patterns for better matching
    patterns = [
        r'\b(\d{13,19})\|(\d{1,2})\|(\d{2,4})\|(\d{3,4})\b',
        r'\b(\d{13,19})\s*\|\s*(\d{1,2})\s*\|\s*(\d{2,4})\s*\|\s*(\d{3,4})\b',
        r'(\d{13,19})\s*[\|\/\-:\s]\s*(\d{1,2})\s*[\|\/\-:\s]\s*(\d{2,4})\s*[\|\/\-:\s]\s*(\d{3,4})',
        r'(\d{4})\s*(\d{4})\s*(\d{4})\s*(\d{4})\s*[\|\/\-:\s]\s*(\d{1,2})\s*[\|\/\-:\s]\s*(\d{2,4})\s*[\|\/\-:\s]\s*(\d{3,4})',
    ]

    credit_cards = []
    for pattern in patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            if len(match) == 4:
                card_number, month, year, cvv = match
                card_number = re.sub(r'[\s\-]', '', card_number)
            elif len(match) == 7:
                card1, card2, card3, card4, month, year, cvv = match
                card_number = card1 + card2 + card3 + card4
            else:
                continue

            # Validation
            if not (13 <= len(card_number) <= 19):
                continue

            try:
                month_int = int(month)
                if not (1 <= month_int <= 12):
                    continue
            except ValueError:
                continue

            if len(year) == 4:
                year = year[-2:]
            elif len(year) != 2:
                continue

            if not (3 <= len(cvv) <= 4):
                continue

            credit_cards.append(f"{card_number}|{month.zfill(2)}|{year}|{cvv}")

    # Remove duplicates while preserving order
    return list(dict.fromkeys(credit_cards))

def format_card_message_enhanced(cc_data, bin_info):
    """Enhanced message formatting"""
    scheme = "UNKNOWN"
    card_type = "UNKNOWN"
    brand = "UNKNOWN"
    bank_name = "UNKNOWN BANK"
    country_name = "UNKNOWN"
    country_emoji = "🌍"
    bin_number = cc_data.split('|')[0][:6]

    if bin_info:
        scheme = bin_info.get('scheme', 'UNKNOWN').upper()
        card_type = bin_info.get('type', 'UNKNOWN').upper()
        brand = bin_info.get('brand', 'UNKNOWN').upper()
        bank_name = bin_info.get('bank', 'UNKNOWN BANK')
        country_name = bin_info.get('country_name', 'UNKNOWN')
        country_emoji = bin_info.get('country_flag', '🌍')
    else:
        # Fallback to unknowns if no bin info
        scheme = brand = "UNKNOWN"

    # Timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    message = f"""[ϟ] 𝐀𝐩𝐩𝐫𝐨𝐯𝐞𝐝 𝐒𝐜𝐫𝐚𝐩𝐩𝐞𝐫
━━━━━━━━━━━━━
[ϟ] 𝗖𝗖 - <code>{cc_data}</code> 
[ϟ] 𝗦𝘁𝗮𝘁𝘂𝘀 : APPROVED ✅
[ϟ] 𝗚𝗮𝘁𝗲 - Stripe Auth
━━━━━━━━━━━━━
[ϟ] 𝗕𝗶𝗻 : {bin_number}
[ϟ] 𝗖𝗼𝘂𝗻𝘁𝗿𝘆 : {country_name} {country_emoji}
[ϟ] 𝗜𝘀𝘀𝘂𝗲𝗿 : {bank_name}
[ϟ] 𝗧𝘆𝗽𝗲 : {card_type} - {brand}
━━━━━━━━━━━━━
[ϟ] 𝗧𝗶𝗺𝗲 : {timestamp}
[ϟ] 𝗦𝗰𝗿𝗮𝗽𝗽𝗲𝗱 𝗕𝘆 : @Bunny"""
    return message

async def process_single_card_enhanced(cc_data):
    """Process single card with enhanced error handling"""
    async with card_semaphore:
        try:
            logger.info(f"🔄 PROCESSING CC: {cc_data[:12]}***")
            bin_number = cc_data.split('|')[0][:6]

            # New bin lookup from antipublic
            try:
                bin_info = await asyncio.wait_for(
                    get_bin_info_antipublic(bin_number),
                    timeout=BIN_TIMEOUT
                )
            except asyncio.TimeoutError:
                logger.warning(f"⏰ BIN lookup timeout for {bin_number}")
                bin_info = None

            if bin_info:
                logger.info(f"✅ BIN lookup successful: {bin_info.get('brand', 'UNKNOWN')} - {bin_info.get('country_name', 'UNKNOWN')}")
            else:
                logger.warning(f"⚠️ BIN lookup failed for {bin_number}")

            formatted_message = format_card_message_enhanced(cc_data, bin_info)

            # Send with enhanced error handling
            success = await send_to_target_channels_enhanced(formatted_message, cc_data)

            if success:
                logger.info(f"✅ Successfully processed and sent CC: {cc_data[:12]}***")
            else:
                logger.error(f"❌ Failed to send CC: {cc_data[:12]}***")

            # Small delay between card processing
            await asyncio.sleep(PROCESS_DELAY)

        except Exception as e:
            logger.error(f"❌ Error processing CC {cc_data}: {e}")
            stats['errors'] += 1

async def process_message_for_ccs_enhanced(message):
    """Enhanced message processing"""
    global processed_messages
    try:
        if message.id in processed_messages:
            return

        processed_messages.add(message.id)
        stats['messages_processed'] += 1

        # Memory management
        if len(processed_messages) > MAX_PROCESSED_MESSAGES:
            processed_messages = set(list(processed_messages)[-2500:])

        text = message.text or message.caption
        if not text:
            return

        logger.info(f"📝 PROCESSING MESSAGE {message.id}: {text[:50]}...")
        credit_cards = extract_credit_cards_enhanced(text)

        if not credit_cards:
            return

        logger.info(f"🎯 FOUND {len(credit_cards)} CARDS in message {message.id}")
        stats['cards_found'] += len(credit_cards)

        # Process cards sequentially to avoid overwhelming
        for cc_data in credit_cards:
            await process_single_card_enhanced(cc_data)

    except Exception as e:
        logger.error(f"❌ Error processing message {message.id}: {e}")
        stats['errors'] += 1

async def poll_for_new_messages_enhanced():
    """Enhanced polling with better error handling"""
    global last_processed_message_ids, is_running
    logger.info("🔄 Starting enhanced polling...")

    # Initialize last processed message IDs
    for group_id in SOURCE_GROUPS:
        try:
            async for message in user.get_chat_history(group_id, limit=1):
                last_processed_message_ids[group_id] = message.id
                logger.info(f"📍 Group {group_id} starting from message ID: {message.id}")
                break
        except Exception as e:
            logger.error(f"❌ Error getting initial message ID for group {group_id}: {e}")
            last_processed_message_ids[group_id] = 0

    while is_running:
        try:
            # Poll each group
            for group_id in SOURCE_GROUPS:
                await poll_single_group_enhanced(group_id)
                await asyncio.sleep(0.5)

            # Polling interval
            await asyncio.sleep(POLLING_INTERVAL)

        except Exception as e:
            logger.error(f"❌ Error in polling loop: {e}")
            stats['errors'] += 1
            await asyncio.sleep(5)

async def poll_single_group_enhanced(group_id):
    """Enhanced polling for single group"""
    try:
        last_id = last_processed_message_ids.get(group_id, 0)

        new_messages = []
        message_count = 0

        async for message in user.get_chat_history(group_id, limit=MESSAGE_BATCH_SIZE):
            message_count += 1
            if message.id <= last_id:
                break
            new_messages.append(message)

        new_messages.reverse()

        if new_messages:
            logger.info(f"📨 Group {group_id}: FOUND {len(new_messages)} NEW MESSAGES")

            # Process messages sequentially
            for message in new_messages:
                await process_message_for_ccs_enhanced(message)
                last_processed_message_ids[group_id] = max(last_processed_message_ids[group_id], message.id)
                await asyncio.sleep(0.1)

        else:
            logger.debug(f"📭 Group {group_id}: No new messages")

    except Exception as e:
        logger.error(f"❌ Error polling group {group_id}: {e}")
        stats['errors'] += 1

# Real-time message handler - FIXED SYNTAX
@user.on_message(filters.chat(SOURCE_GROUPS))
async def realtime_message_handler_enhanced(client, message):
    """Enhanced real-time handler"""
    logger.info(f"⚡ REAL-TIME MESSAGE: {message.id} from group {message.chat.id}")
    # Process immediately but don't block
    asyncio.create_task(process_message_for_ccs_enhanced(message))

async def print_stats_enhanced():
    """Print enhanced statistics"""
    while is_running:
        await asyncio.sleep(60)  # Every minute
        if stats['start_time']:
            uptime = datetime.now() - stats['start_time']
            logger.info(f"📊 CC MONITOR STATS - Uptime: {uptime}")
            logger.info(f"📨 Messages: {stats['messages_processed']}")
            logger.info(f"🎯 Cards Found: {stats['cards_found']}")
            logger.info(f"✅ Cards Sent: {stats['cards_sent']}")
            logger.info(f"🔄 Duplicates: {stats['cards_duplicated']}")
            logger.info(f"❌ Send Failures: {stats['send_failures']}")
            logger.info(f"🔍 BIN Success: {stats['bin_lookups_success']}")
            logger.info(f"💾 Cache Size: {len(bin_cache)}")
            logger.info(f"📢 Active Channels: {len(accessible_channels)}")
            logger.info(f"❌ Total Errors: {stats['errors']}")

def signal_handler(signum, frame):
    global is_running
    logger.info(f"🛑 SHUTDOWN SIGNAL {signum} - Stopping monitor...")
    is_running = False

async def main():
    global is_running
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        logger.info("🚀 STARTING ENHANCED CC MONITOR...")
        logger.info(f"⚙️ SETTINGS:")
        logger.info(f"   📡 Monitoring {len(SOURCE_GROUPS)} groups: {SOURCE_GROUPS}")
        logger.info(f"   📤 Target channels: {TARGET_CHANNELS}")
        logger.info(f"   ⏱️ Polling interval: {POLLING_INTERVAL}s")
        logger.info(f"   ⏳ Send delay: {SEND_DELAY}s")
        logger.info(f"   🔄 Retry attempts: {RETRY_ATTEMPTS}")

        stats['start_time'] = datetime.now()

        await user.start()
        logger.info("✅ User client started successfully!")
        await asyncio.sleep(2)

        # Discover accessible channels
        channel_discovery_ok = await discover_accessible_channels()
        if not channel_discovery_ok:
            logger.error("❌ No accessible channels found!")
            logger.info("💡 Please add your bot to the target channels as admin and restart")
            return

        # Test channel sending
        channel_test_ok = await test_channel_sending()
        if not channel_test_ok:
            logger.error("❌ Channel sending test failed!")
            return
        else:
            logger.info("✅ All channel tests passed!")

        # Start background tasks
        logger.info("🚀 Starting background tasks...")
        polling_task = asyncio.create_task(poll_for_new_messages_enhanced())
        stats_task = asyncio.create_task(print_stats_enhanced())

        try:
            logger.info("✅ CC MONITOR FULLY ACTIVE!")
            logger.info("🔍 Monitoring for credit cards...")
            logger.info(f"📤 Ready to send to {len(accessible_channels)} channels...")
            await idle()
        finally:
            # Cleanup
            polling_task.cancel()
            stats_task.cancel()
            try:
                await asyncio.gather(polling_task, stats_task, return_exceptions=True)
            except:
                pass

    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        stats['errors'] += 1
    finally:
        logger.info("🛑 Stopping client...")
        try:
            if user.is_connected:
                await user.stop()
                logger.info("✅ Client stopped successfully")
        except Exception as e:
            logger.error(f"❌ Error stopping client: {e}")

        # Final stats
        if stats['start_time']:
            uptime = datetime.now() - stats['start_time']
            logger.info(f"📊 FINAL STATS:")
            logger.info(f"   ⏱️ Total Uptime: {uptime}")
            logger.info(f"   📨 Messages Processed: {stats['messages_processed']}")
            logger.info(f"   🎯 Cards Found: {stats['cards_found']}")
            logger.info(f"   ✅ Cards Sent: {stats['cards_sent']}")
            logger.info(f"   🔄 Duplicates Blocked: {stats['cards_duplicated']}")
            logger.info(f"   ❌ Send Failures: {stats['send_failures']}")
            logger.info(f"   💾 BINs Cached: {len(bin_cache)}")
            logger.info(f"   ❌ Total Errors: {stats['errors']}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 CC MONITOR STOPPED BY USER")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        sys.exit(1)
