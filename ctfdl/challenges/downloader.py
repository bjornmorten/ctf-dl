import asyncio
from pathlib import Path

from ctfbridge.base.client import CTFClient
from ctfbridge.exceptions import (
    LoginError,
    MissingAuthMethodError,
    NotAuthenticatedError,
    UnknownBaseURLError,
    UnknownPlatformError,
)
from ctfbridge.models.challenge import Challenge, ProgressData

from ctfdl.challenges.client import get_authenticated_client
from ctfdl.core import EventEmitter, ExportConfig
from ctfdl.core.models import ChallengeEntry
from ctfdl.rendering.context import TemplateEngineContext
from ctfdl.rendering.engine import TemplateEngine


async def download_challenges(config: ExportConfig, emitter: EventEmitter) -> tuple[bool, list]:
    try:
        await emitter.emit("connect_start", url=config.url)
        client = await get_authenticated_client(
            config.url, config.username, config.password, config.token
        )
        await emitter.emit("connect_success")
    except UnknownPlatformError:
        await emitter.emit(
            "connect_fail",
            reason="Unsupported platform. You may suggest adding support here: https://github.com/bjornmorten/ctfbridge/issues",
        )
        return False, []
    except UnknownBaseURLError:
        await emitter.emit(
            "connect_fail",
            reason=(
                "Platform was identified, but base URL could not be determined. "
                "If you believe this is an error, you may open an issue here: "
                "https://github.com/bjornmorten/ctfbridge/issues"
            ),
        )
        return False, []
    except LoginError:
        await emitter.emit("connect_fail", reason="Authentication failed")
        return False, []
    except MissingAuthMethodError:
        await emitter.emit("connect_fail", reason="Invalid authentication type")
        return False, []

    challenges_iterator = client.challenges.iter_all(
        categories=config.categories,
        min_points=config.min_points,
        max_points=config.max_points,
        solved=True if config.solved else False if config.unsolved else None,
        detailed=True,
        enrich=True,
    )

    template_engine = TemplateEngineContext.get()
    output_dir = config.output
    output_dir.mkdir(parents=True, exist_ok=True)
    all_challenges_data = []

    sem = asyncio.Semaphore(config.parallel)
    tasks = []
    challenge_count = 0

    async def process(chal: Challenge):
        try:
            await emitter.emit("challenge_start", challenge=chal)

            entry = await process_challenge(
                client,
                emitter,
                chal,
                template_engine,
                config,
                output_dir,
            )
            all_challenges_data.append(entry)
            await emitter.emit("challenge_success", challenge=chal)
        except Exception as e:
            await emitter.emit("challenge_fail", challenge=chal, reason=str(e))
        finally:
            await emitter.emit("challenge_complete", challenge=chal)

    async def worker(chal: Challenge):
        async with sem:
            await process(chal)

    await emitter.emit("fetch_start")

    try:
        started = False
        async for chal in challenges_iterator:
            if not started:
                await emitter.emit("download_start")
                started = True

            challenge_count += 1
            task = asyncio.create_task(worker(chal))
            tasks.append(task)
    except NotAuthenticatedError:
        await emitter.emit("authentication_required")
        return False, []

    if challenge_count == 0:
        await emitter.emit("no_challenges_found")
        await emitter.emit("download_complete")
        return False, []

    await asyncio.gather(*tasks)

    await emitter.emit("download_complete")
    return True, all_challenges_data


async def process_challenge(
    client: CTFClient,
    emitter: EventEmitter,
    chal: Challenge,
    template_engine: TemplateEngine,
    config: ExportConfig,
    output_dir: Path,
) -> ChallengeEntry:
    rel_path_str = template_engine.render_path(config.folder_template_name, chal)
    chal_folder = output_dir / rel_path_str

    existed_before = chal_folder.exists()

    if existed_before and not config.update:
        await emitter.emit("challenge_skipped", challenge=chal)
        return ChallengeEntry(data=chal, path=Path(rel_path_str))

    async def progress_callback(pd: ProgressData):
        await emitter.emit("attachment_progress", progress_data=pd, challenge=chal)

    chal_folder.mkdir(parents=True, exist_ok=True)
    if not config.no_attachments and chal.attachments:
        files_dir = chal_folder / "files"
        files_dir.mkdir(exist_ok=True)
        chal = await client.attachments.download_all(
            chal,
            save_dir=str(files_dir),
            progress=progress_callback,
            concurrency=config.parallel,
        )
    template_engine.render_challenge(config.variant_name, chal, chal_folder)

    await emitter.emit("challenge_downloaded", challenge=chal, updated=existed_before)

    return ChallengeEntry(
        data=chal,
        path=Path(rel_path_str),
        updated=existed_before,
    )
