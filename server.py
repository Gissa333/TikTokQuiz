import asyncio
import websockets
import json
import os
import requests
import html
from tiktok_live_events import TikTokLive
from aiohttp import web

# دالة لجلب الأسئلة من Open Trivia Database
def fetch_questions():
    # الفئة 17 هي "Science & Nature"، الكمية 10، النوع متعدد الخيارات
    url = "https://opentdb.com/api.php?amount=10&category=17&type=multiple"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        
        if data['response_code'] == 0:
            questions = []
            for item in data['results']:
                # فك تشفير النصوص (لأن OpenTDB ترجع رموز HTML)
                q_text = html.unescape(item['question'])
                correct = html.unescape(item['correct_answer'])
                incorrect = [html.unescape(ans) for ans in item['incorrect_answers']]
                
                # دمج الإجابات الخاطئة والصحيحة وخلطها
                all_answers = incorrect + [correct]
                import random
                random.shuffle(all_answers)
                
                # تحديد الحرف الصحيح (A, B, C, D)
                correct_index = all_answers.index(correct)
                correct_letter = ["A", "B", "C", "D"][correct_index]
                
                # تنسيق الخيارات
                opts = [f"A) {all_answers[0]}", f"B) {all_answers[1]}", f"C) {all_answers[2]}", f"D) {all_answers[3]}"]
                
                questions.append({
                    "q": q_text,
                    "opts": opts,
                    "ans": correct_letter
                })
            return questions
    except Exception as e:
        print(f"Error fetching questions: {e}")
    
    # في حال فشل الاتصال، نرجع لأسئلة احتياطية
    return [
        {"q": "ما هي عاصمة فرنسا؟", "opts": ["A) لندن", "B) باريس", "C) روما", "D) مدريد"], "ans": "B"},
        {"q": "ما هو أكبر كوكب في المجموعة الشمسية؟", "opts": ["A) الأرض", "B) المريخ", "C) المشتري", "D) زحل"], "ans": "C"}
    ]

QUESTIONS = fetch_questions()
current_q_index = 0
clients = set()
main_loop = None
timer_task = None
winner_found = False
live = TikTokLive("7_upu")

async def question_timer():
    global current_q_index, winner_found
    await asyncio.sleep(30)
    if not winner_found:
        for ws in clients.copy():
            asyncio.run_coroutine_threadsafe(ws.send(json.dumps({"type": "timeout"})), main_loop)
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
            asyncio.run_coroutine_threadsafe(ws.send(q_json), main_loop)
        
        if timer_task:
            timer_task.cancel()
        timer_task = asyncio.create_task(question_timer())
    else:
        for ws in clients.copy():
            asyncio.run_coroutine_threadsafe(ws.send(json.dumps({"type": "end"})), main_loop)

@live.on('chat')
def on_chat(e):
    global winner_found
    if winner_found or current_q_index >= len(QUESTIONS):
        return 
        
    current_ans = QUESTIONS[current_q_index]["ans"]
    comment = e['comment'].strip().upper()
    
    if comment == current_ans:
        winner_found = True
        if timer_task:
            main_loop.call_soon_threadsafe(timer_task.cancel)
            
        user = e['user']['uniqueId']
        nickname = e['user'].get('nickname', user)
        winner_msg = f"{nickname} (@{user})"
        
        msg_json = json.dumps({"type": "winner", "name": winner_msg})
        for ws in clients.copy():
            asyncio.run_coroutine_threadsafe(ws.send(msg_json), main_loop)
        
        asyncio.run_coroutine_threadsafe(next_q(), main_loop)

async def websocket_handler(request):
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
            pass
    except:
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
    print(f"🚀 الخادم جاهز على المنفذ {port} مع أسئلة علمية أونلاين")
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())