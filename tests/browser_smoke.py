from pathlib import Path
from playwright.sync_api import sync_playwright, expect, TimeoutError as PlaywrightTimeout
import urllib.request, json, os, ssl
ssl_context=ssl._create_unverified_context()  # local test-only certificate
base="https://127.0.0.1:18080"
out=Path("test-screenshots");out.mkdir(exist_ok=True)
root=Path(__file__).resolve().parents[1]
def init(id):
    return urllib.request.urlopen(f"{base}/test-init/{id}",context=ssl_context).read().decode()
def session(b,id,w,h):
    ctx=b.new_context(viewport={"width":w,"height":h},device_scale_factor=1,is_mobile=w<600,has_touch=w<600,accept_downloads=True,ignore_https_errors=True)
    p=ctx.new_page();errors=[]
    p.on("pageerror",lambda e: errors.append(str(e)))
    p.on("response",lambda r: print("media response",r.status,r.url.split("?")[0]) if "/file/" in r.url else None)
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
    if cover:
        choice=p.locator(f'.cms-cover-choice:has([value="{cover}"])')
        choice.click()
        expect(choice.locator('input')).to_be_checked()
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
def playback(p,ctx,label):
    player=p.locator(".routing-player")
    v=player.locator("video")
    launch=player.locator(".player-launch")
    launch.scroll_into_view_if_needed()
    box=player.bounding_box()
    print(label,"codec support",v.evaluate("""v=>({mp4:v.canPlayType('video/mp4'),h264:v.canPlayType('video/mp4; codecs="avc1.42E01E"')})"""),flush=True)
    media=v.get_attribute("data-video-src")
    response=ctx.request.get(base+media,headers={"Range":"bytes=0-31"})
    assert response.status==206 and response.headers.get("content-type","").startswith("video/mp4"),f"{label} video range {response.status} {response.headers}"
    assert len(response.body())==32,"wrong range bytes"
    launch.click()
    try:
        p.wait_for_function("document.querySelector('video').currentTime > 0.1",timeout=20000)
    except PlaywrightTimeout:
        print(label,"video diagnostic",v.evaluate("""v=>({error:v.error?.code||0,message:v.error?.message||'',readyState:v.readyState,networkState:v.networkState,paused:v.paused,duration:Number.isFinite(v.duration)?v.duration:null,currentTime:v.currentTime,buffered:v.buffered.length})"""),flush=True)
        shot(p,label+"-playback-error")
        raise
    after=player.bounding_box()
    assert all(abs(box[k]-after[k])<1 for k in ("width","height")),"player layout shifted"
    shot(p,label+"-playing")
    duration=v.evaluate("v=>{v.pause();return v.duration}")
    assert duration>0.4,"invalid fixture duration"
    target=duration*0.6
    v.evaluate("(v,t)=>{v.currentTime=t}",target)
    p.wait_for_function("t=>{const v=document.querySelector('video');return !v.seeking&&Math.abs(v.currentTime-t)<0.25}",arg=target,timeout=15000)
    print(label,"playback and seek passed",flush=True)
with sync_playwright() as pw:
    b=pw.chromium.launch(headless=True,channel="chrome")
    ctx,p,errors=session(b,42,1440,940)
    shot(p,"desktop-profile")
    admin(p)
    p.locator('[data-action="cmsproject:industry"]').click()
    expect(p.locator("#cms-title")).to_have_value("Отраслевое деление")
    p.locator('[data-action="cmsproject:calendar"]').click()
    expect(p.locator("#cms-title")).to_have_value("Календарное планирование")
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
    nav(p,"projects")
    p.locator('.project-tile.calendar .project-pin').click()
    expect(p.locator('.project-tile.calendar .project-pin')).to_have_attribute("aria-pressed","true")
    nav(p,"profile")
    expect(p.locator("#cabinet-work")).to_contain_text("Материалы моего проекта")
    folder=p.locator(".cabinet-folder").filter(has_text="Документы").first
    folder.locator("summary").click()
    row=folder.locator(".cabinet-material-row").filter(has_text="Календарное планирование")
    row.locator(".cabinet-bookmark").click()
    expect(row.locator(".cabinet-bookmark")).to_have_attribute("aria-pressed","true")
    shot(p,"mobile-cabinet-folders")
    p.locator('[data-go="cabinet-saved"]').click()
    expect(p.locator("#cabinet-saved-content")).to_contain_text("Календарное планирование")
    p.reload()
    expect(p.locator("#cabinet-work")).to_contain_text("Сохранённые материалы")
    p.locator('[data-go="cabinet-saved"]').click()
    expect(p.locator("#cabinet-saved-content .cabinet-folder")).to_be_visible()
    p.locator("#cabinet-saved-content summary").click()
    expect(p.locator("#cabinet-saved-content .cabinet-material-row").first.locator(".cabinet-bookmark")).to_have_attribute("aria-pressed","true")
    shot(p,"mobile-cabinet-saved")
    p.locator("#cabinet-saved-content .cabinet-material-title").first.click()
    expect(p.locator(".feed-heading h1")).to_have_text("Календарное планирование")
    expect(p.locator(".material-cover").first).to_be_visible()
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
    media=p.locator(".material-cover").first.get_attribute("href")
    assert any(c["name"]=="__Host-routing-media" for c in ctx.cookies()),"Secure media cookie absent"
    response=ctx.request.get(base+media,headers={"Range":"bytes=0-3"})
    assert response.status==206, f"media range failed: {response.status} {response.text()[:300]}"
    with p.expect_download(timeout=15000) as dl:p.locator(".material-cover").first.click()
    assert dl.value.failure() is None
    assert dl.value.path().read_bytes()==b"Demo presentation"
    assert not errors,errors
    ctx.close()
    ctx,p,errors=session(b,42,390,844)
    admin(p);shot(p,"mobile-admin")
    add(p,"image","Временная иллюстрация",str(root/"web/assets/instruction.webp"))
    row=p.locator(".cms-order-row").filter(has_text="Временная иллюстрация")
    image=row.locator("img")
    image.scroll_into_view_if_needed()
    expect(image).to_have_js_property("complete",True)
    p.wait_for_function("document.querySelector('.cms-order-row img')?.naturalWidth > 0")
    image_url=image.get_attribute("src")
    script_row=p.locator(".cms-order-row").filter(has_text="Скрипт работы")
    script_row.locator(".cms-material-edit").click()
    expect(p.get_by_role("radio",name="Скрипт работы")).to_be_checked()
    choice=p.locator('.cms-cover-choice:has([value="analytics"])')
    choice.click()
    choice.scroll_into_view_if_needed()
    shot(p,"mobile-cover-picker")
    art_style="el=>{const css=getComputedStyle(el);return [css.backgroundImage,css.backgroundSize,css.backgroundPosition]}"
    selected_art=choice.locator(".cms-preview").evaluate(art_style)
    p.locator("#cms-title").fill("Unsaved edit")
    row.get_by_role("button",name="Удалить «Временная иллюстрация»").click()
    row.get_by_role("button",name="Отмена").click()
    expect(row).to_be_visible()
    row.get_by_role("button",name="Удалить «Временная иллюстрация»").click()
    shot(p,"mobile-delete-material")
    row.get_by_role("button",name="Да, удалить").click()
    expect(row).to_have_count(0)
    assert ctx.request.get(base+image_url).status==404
    expect(p.locator("#cms-title")).to_have_value("Unsaved edit")
    expect(choice.locator("input")).to_be_checked()
    assert p.evaluate("""async () => {
        const p=await cmsRequest('/api/content/calendar?edit=1');
        return p.title==='Календарное планирование' && p.blocks.find(b=>b.title==='Скрипт работы').cover==='script'
    }"""),"deletion committed unrelated edits"
    p.locator("#cms-title").fill("Календарное планирование")
    p.locator('#cms-form [type=submit]').click()
    p.locator('[data-action="cmssave"]').click()
    expect(p.locator("#toast")).to_contain_text("Изменения сохранены")
    p.reload()
    admin(p)
    expect(p.locator("#cms-order")).not_to_contain_text("Временная иллюстрация")
    script_row.locator(".cms-material-edit").click()
    expect(p.get_by_role("radio",name="Аналитика")).to_be_checked()
    p.locator("#cms-title").fill("Unsaved edit")
    p.once("dialog",lambda d:d.dismiss());nav(p,"projects")
    expect(p.locator("#cms-title")).to_have_value("Unsaved edit")
    p.once("dialog",lambda d:d.accept());nav(p,"projects")
    project(p)
    published_art=p.locator(".material-cover").filter(has_text="Скрипт работы").locator(".cover-art")
    assert published_art.evaluate(art_style)==selected_art
    assert not errors,errors;ctx.close()
    ctx,p,errors=session(b,99,390,844)
    expect(p.locator(".access-panel")).to_contain_text("только участникам рабочего чата")
    assert not errors,errors;ctx.close()
    # Keep both playback checks independent; a failure on one viewport does not mask the other.
    playback_failures=[]
    for uid,width,height,label in [(4,390,844,"mobile"),(42,1440,940,"desktop")]:
        ctx,p,errors=session(b,uid,width,height)
        try:
            project(p)
            playback(p,ctx,label)
            assert not errors,errors
        except Exception as e:
            playback_failures.append(f"{label}: {e}")
        finally:
            ctx.close()
    assert not playback_failures,playback_failures
    # Mixed ordered blocks and the employee preview (run after single-player checks).
    ctx,p,errors=session(b,42,390,844)
    admin(p)
    p.locator('.cms-insert').first.click()
    add(p,"text","Первый блок",text="Текст перед остальными материалами")
    expect(p.locator('.cms-order-row').first).to_contain_text("Первый блок")
    p.locator('.cms-insert').nth(1).click()
    add(p,"video","Второе видео",str(root/"demo/overview.mp4"))
    expect(p.locator('.cms-order-row').nth(1)).to_contain_text("Второе видео")
    p.locator('.cms-insert').nth(2).click()
    add(p,"text","Текст после видео",text="Описание второго шага")
    p.locator('.cms-insert').last.click()
    add(p,"link","Ссылка с обложкой",url="https://yandex.ru/routing/",cover="instruction")
    p.locator('[data-action="cmskind:link"]').click()
    expect(p.locator('[name=linkStyle][value=compact]')).to_be_checked()
    expect(p.locator('[name=cover][value=website]')).to_be_checked()
    p.locator('#cms-form [name=title]').fill("Сайт с глобусом")
    p.locator('#cms-form [name=url]').fill("https://example.com/")
    p.locator('[name=linkStyle][value=wide]').check()
    expect(p.locator('#cms-link-sample .feed-link-wide .link-art-website')).to_be_visible()
    p.locator('#cms-form [type=submit]').click()
    expect(p.locator('#cms-order')).to_contain_text("Сайт с глобусом")
    p.locator('#cms-form [name=title]').fill("Простая ссылка")
    p.locator('#cms-form [name=url]').fill("https://example.org/")
    p.locator('[name=linkStyle][value=minimal]').check()
    expect(p.locator('.cms-link-covers')).to_be_hidden()
    p.locator('#cms-form [type=submit]').click()
    expect(p.locator('#cms-order')).to_contain_text("Простая ссылка")
    p.locator('.cms-order-row').filter(has_text="Ссылка с обложкой").locator('[data-action^="cmsduplicate:"]').click()
    expect(p.locator('.cms-order-row').filter(has_text="Ссылка с обложкой")).to_have_count(2)
    expect(p.locator('.cms-order-row').filter(has_text="Ссылка с обложкой").last).to_contain_text("Черновик")
    p.locator('[data-action="cmspreview"]').click()
    expect(p.locator('.cms-live-preview .routing-player')).to_have_count(2)
    expect(p.locator('.cms-live-preview .feed-link-art.cover-instruction')).to_have_count(1)
    p.locator('.cms-live-preview').scroll_into_view_if_needed()
    shot(p,"mobile-builder-preview")
    p.get_by_role("button",name="Закрыть",exact=True).click()
    p.locator('#cms-order-heading').scroll_into_view_if_needed()
    shot(p,"mobile-builder-order")
    nav(p,"projects");p.locator('[data-action="projectdocs:calendar"]').click()
    expect(p.locator('.routing-player')).to_have_count(2)
    expect(p.locator('.cms-material-item').nth(0)).to_contain_text("Первый блок")
    expect(p.locator('.cms-material-item').nth(2)).to_contain_text("Текст после видео")
    expect(p.locator('.feed-link-art.cover-instruction')).to_have_count(1)
    expect(p.locator('.feed-link-wide').filter(has_text="Сайт с глобусом").locator('.link-art-website')).to_have_count(1)
    expect(p.locator('.feed-link-minimal').filter(has_text="Простая ссылка").locator('.feed-link-art')).to_have_count(0)
    p.locator('.feed-link-wide').filter(has_text="Сайт с глобусом").scroll_into_view_if_needed()
    shot(p,"mobile-link-formats")
    shot(p,"mobile-builder-feed")
    assert not errors,errors;ctx.close()
    b.close()
print("Browser checks passed: real API, admin upload, draft visibility, download, desktop/mobile layout and playback.")
