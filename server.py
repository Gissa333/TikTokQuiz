import asyncio
import websockets
import json
import os
import requests
import html
import random
from tiktok_live_events import TikTokLive
from aiohttp import web

def fetch_questions():
    url = "https://opentdb.com/api.php?amount=10&category=17&type=multiple"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        if data['response_code'] == 0:
            questions = []
            for item in data['results']:
                q_text = html.unescape(item['question'])
                correct = html.unescape(item['correct_answer'])
                incorrect = [html.unescape(ans) for ans in item['incorrect_answers']]
                all_answers = incorrect + [correct]
                random.shuffle(all_answers)
                correct_index = all_answers.index(correct)
                correct_letter = ["A", "B", "C", "D"][correct_index]
                opts = [f"A) {all_answers[0]}", f"B) {all_answers[1]}", f"C) {all_answers[2]}", f"D) {all_answers[3]}"]
                questions.append({"q": q_text, "opts": opts, "ans": correct_letter})
            return questions
    except Exception as e:
        print(f"Error fetching questions: {e}")
    return [
        {"q": "ما هي عاصمة فرنسا؟", "opts": ["A) لندن", "B) باريس", "C) روما", "D) مدريد"], "ans": "B"}
    ]

QUESTIONS = fetch_questions()
current_q_index = 0
clients = set()
main_loop = None
timer_task = None
winner_found = False
live = TikTokLive("7_upu")
scores = {}

async def question_timer():
    global current_q_index, winner_found
    await asyncio.sleep(30)
    if not winner_found:
        for ws in clients.copy():
            asyncio.run_coroutine_threadsafe(ws.send_str(json.dumps({"type": "timeout"})), main_loop)
        await next_q()

async def next_q():
    global current_q_index, winner_found, timer_task
    winner_found = False
    current_q_index += 1
    await asyncio.sleep(3)
    
    if current_q_index < len(QUESTIONS):
        new_q = QUESTIONS[current_q_index]
        q_json = json.dumps({"type": "new_question", "question": new_q["q"], "options": new_q["opts"]})
        for ws in clients.copy():
            asyncio.run_coroutine_threadsafe(ws.send_str(q_json), main_loop)
        
        if timer_task:
            timer_task.cancel()
        timer_task = asyncio.create_task(question_timer())
    else:
        for ws in clients.copy():
            asyncio.run_coroutine_threadsafe(ws.send_str(json.dumps({"type": "end"})), main_loop)

async def process_answer(user_name, answer):
    global winner_found
    if winner_found or current_q_index >= len(QUESTIONS):
        return
        
    current_ans = QUESTIONS[current_q_index]["ans"]
    if answer == current_ans:
        winner_found = True
        if timer_task:
            main_loop.call_soon_threadsafe(timer_task.cancel)
            
        scores[user_name] = scores.get(user_name, 0) + 10
        current_score = scores[user_name]
        
        winner_msg = f"{user_name} | Coins: {current_score} 🪙"
        
        msg_json = json.dumps({"type": "winner", "name": winner_msg})
        for ws in clients.copy():
            asyncio.run_coroutine_threadsafe(ws.send_str(msg_json), main_loop)
        
        await next_q()

@live.on('chat')
def on_chat(e):
    comment = e['comment'].strip().upper()
    user = e['user'].get('nickname', e['user']['uniqueId'])
    asyncio.run_coroutine_threadsafe(process_answer(user, comment), main_loop)

async def websocket_handler(request):
    global main_loop, timer_task
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    clients.add(ws)
    
    if current_q_index < len(QUESTIONS):
        q = QUESTIONS[current_q_index]
        await ws.send_str(json.dumps({"type": "new_question", "question": q["q"], "options": q["opts"]}))
        if timer_task is None:
            timer_task = asyncio.create_task(question_timer())
    else:
        await ws.send_str(json.dumps({"type": "end"}))
        
    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                data = json.loads(msg.data)
                if data.get("type") == "app_answer":
                    # هنا التعديل: استخدام create_task لمعالجة الإجابة فوراً
                    asyncio.create_task(process_answer(data.get("user"), data.get("answer")))
    except:
        pass
    finally:
        clients.remove(ws)
    return ws

async def main():
    global main_loop
    main_loop = asyncio.get_running_loop()
    asyncio.create_task(live.run())
    
    app = web.Application()
    app.router.add_get('/', websocket_handler)
    
    port = int(os.environ.get("PORT", 10000))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"🚀 الخادم جاهز على المنفذ {port} مع معالجة فورية للإجابات")
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())