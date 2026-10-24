import asyncio
import websockets
import json
import os
from tiktok_live_events import TikTokLive

# قراءة الأسئلة
with open('questions.json', 'r', encoding='utf-8') as f:
    QUESTIONS = json.load(f)

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

async def handler(websocket):
    global main_loop, timer_task
    clients.add(websocket)
    
    if current_q_index < len(QUESTIONS):
        q = QUESTIONS[current_q_index]
        await websocket.send(json.dumps({"type": "new_question", "question": q["q"], "options": q["opts"]}))
        if timer_task is None:
            timer_task = asyncio.create_task(question_timer())
    else:
        await websocket.send(json.dumps({"type": "end"}))
        
    try:
        await asyncio.Future() 
    except:
        clients.remove(websocket)

async def main():
    global main_loop
    main_loop = asyncio.get_running_loop()
    
    # الاستضافة السحابية تتطلب الاستماع على 0.0.0.0 وليس 127.0.0.1
    port = int(os.environ.get("PORT", 10000))
    async with websockets.serve(handler, "0.0.0.0", port):
        print(f"🚀 الخادم جاهز على المنفذ {port}")
        asyncio.create_task(live.run())
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())