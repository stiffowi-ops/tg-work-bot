from pathlib import Path
from playwright.sync_api import sync_playwright, expect
import urllib.request, json, os
base="http://127.0.0.1:18080"
out=Path("test-screenshots");out.mkdir(exist_ok=True)
root=Path(__file__).resolve().parents[1]
def init(id):
    return urllib.request.urlopen(f"{base}/test-init/{id}").read().decode()
def session(b,id,w,h):
    ctx=b.new_context(viewport={"width":w,"height":h},device_scale_factor=1,is_mobile=w<600,has_touch=w<600,accept_downloads=True)
    p=ctx.new_page();errors=[]
    p.on("pageerror",lambda e: errors.append(str(e)))
    p.route("https://telegram.org/js/telegram-web-app.js",lambda r:r.fulfill(content_type="application/javascript",body="window.Telegram={WebApp:{initData:"+json.dumps(init(id))+",ready(){},expand(){}}};"))
    p.goto(base)
    if id!=99: expect(p.locator(".employee-card")).to_be_visible(timeout=15000)
    return ctx,p,errors
def nav(p,dest):
    p.locator(f'#mobile [data-go="{dest}"]').click()
def admin(p):
    expect(p.locator('#mobile [data-go="admin"]')).to_be_visible()
    nav(p,"admin");expect(p.locator("#cms-form")).to_be_visible()
def add(p,kind,title,file=None,cover=None,text=None,url=None,published=True):
    p.locator(f'[data-action="cmskind:{kind}"]').click()
    p.locator('#cms-form [name=title]').fill(title)
    if text:p.locator('#cms-form [name=text]').fill(text)
    if url:p.locator('#cms-form [name=url]').fill(url)
    if cover:p.locator('#cms-form [name=cover]').select_option(cover)
    if file:p.locator('#cms-form [name=file]').set_input_files(file)
    if not published:p.locator('#cms-form [name=published]').uncheck()
    p.locator('#cms-form [type=submit]').click()
    expect(p.locator("#cms-order")).to_contain_text(title,timeout=20000)
def project(p):
    nav(p,"projects");p.locator('[data-action="projectdocs:calendar"]').click()
    expect(p.locator(".feed-heading h1")).to_have_text("Календарное планирование")
    expect(p.locator(".routing-player")).to_be_visible(timeout=15000)
def shot(p,name):
    assert p.evaluate("document.documentElement.scrollWidth <= innerWidth+1"), name+" overflow"
    p.screenshot(path=str(out/(name+".png")),full_page=False,animations="disabled")
with sync_playwright() as pw:
    b=pw.chromium.launch(headless=True,channel="chromium")
    ctx,p,errors=session(b,42,1440,940)
    shot(p,"desktop-profile")
    admin(p)
    add(p,"video","Демонстрация продукта",str(root/"demo/overview.mp4"))
    add(p,"document","Календарное планирование",{"name":"presentation.txt","mimeType":"text/plain","buffer":b"Demo presentation"},cover="presentation")
    add(p,"document","Скрипт работы",{"name":"script.txt","mimeType":"text/plain","buffer":b"Demo script"},cover="script")
    add(p,"text","Скрытый черновик",text="Not visible to employees",published=False)
    add(p,"link","Документация",url="https://yandex.ru/routing/")
    # Reorder video before overview using the accessible arrows.
    rows=p.locator(".cms-order-row")
    video=rows.filter(has_text="Демонстрация продукта")
    video.locator('[aria-label="Выше"]').click()
    p.locator('[data-action="cmssave"]').click()
    expect(p.locator("#toast")).to_contain_text("Изменения сохранены")
    shot(p,"desktop-admin")
    nav(p,"projects");shot(p,"desktop-projects")
    project(p);shot(p,"desktop-feed")
    assert not errors,errors
    ctx.close()
    ctx,p,errors=session(b,4,390,844)
    expect(p.locator('#mobile [data-go="admin"]')).to_have_count(0)
    shot(p,"mobile-profile")
    nav(p,"projects");shot(p,"mobile-projects")
    project(p);expect(p.locator("#content")).not_to_contain_text("Скрытый черновик")
    shot(p,"mobile-feed")
    def check_nav():
        assert p.evaluate("""() => {const nav=document.querySelector('#mobile').getBoundingClientRect(), shell=document.querySelector('.shell').getBoundingClientRect();return shell.bottom<=nav.top+1 && nav.bottom<=innerHeight+1 && nav.bottom>=innerHeight-60}"""),"navigation overlaps content or leaves viewport"
    check_nav()
    p.locator(".routing-player").scroll_into_view_if_needed()
    check_nav();shot(p,"mobile-player")
    p.locator(".material-cover").last.scroll_into_view_if_needed()
    check_nav();shot(p,"mobile-documents")
    p.evaluate("document.querySelector('.shell').scrollTop=document.querySelector('.shell').scrollHeight")
    check_nav();shot(p,"mobile-feed-bottom")
    with p.expect_download(timeout=15000) as dl:p.locator(".material-cover").first.click()
    assert dl.value.failure() is None
    assert not errors,errors
    ctx.close()
    ctx,p,errors=session(b,42,390,844)
    admin(p);shot(p,"mobile-admin");assert not errors,errors;ctx.close()
    ctx,p,errors=session(b,99,390,844)
    expect(p.locator(".access-panel")).to_contain_text("только участникам рабочего чата")
    assert not errors,errors;ctx.close()
    # Capture all screens before playback assertions, so a media failure cannot hide mobile screenshots.
    ctx,p,errors=session(b,4,390,844)
    project(p)
    p.locator(".player-launch").scroll_into_view_if_needed()
    box=p.locator(".routing-player").bounding_box()
    p.locator(".player-launch").click();p.wait_for_function("document.querySelector('video').currentTime > 0.1",timeout=20000)
    after=p.locator(".routing-player").bounding_box()
    assert abs(box["width"]-after["width"])<1 and abs(box["height"]-after["height"])<1
    assert not errors,errors;ctx.close()
    ctx,p,errors=session(b,42,1440,940)
    project(p)
    v=p.locator(".routing-player video")
    p.locator(".player-launch").scroll_into_view_if_needed()
    box=p.locator(".routing-player").bounding_box()
    p.locator(".player-launch").click()
    p.wait_for_function("document.querySelector('video').currentTime > 0.1",timeout=20000)
    after=p.locator(".routing-player").bounding_box()
    assert all(abs(box[k]-after[k])<1 for k in ("width","height")),"player layout shifted"
    v.evaluate("(v)=>v.pause()")
    assert not errors,errors;ctx.close()
    b.close()
print("Browser checks passed: real API, admin upload, draft visibility, download, desktop/mobile layout and playback.")
