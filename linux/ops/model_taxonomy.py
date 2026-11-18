"""Which model made an image: a family for the gallery's filter, and a name.

The gallery files every image under one of two groups and a family inside it:

    NAI系      NAI-V5, NAI-V4.5, NAI-V4, NAI-V3, NAI-V1/V2, NAI-未知版本
    本地模型    Illustrious, NoobAI, Pony, SDXL, SD1.5, Anima, Krea2, MOSS-Image,
               Qwen-Image, Flux, Chroma, Z-Image, NewBie, Lumina, HiDream,
               HunyuanImage, SD3, 本地-未识别
    (neither)  未知 - nothing in the file says what made it

A family is the base architecture, because that is what decides how an image
was prompted and which LoRAs fit; the checkpoint inside it (``model``) is the
second level.  NovelAI's own family names keep the ``NAI-`` ids the gallery
already filtered on, so saved filters and links stay valid.

Every rule reads only what the file carries, in the order of how much it can
be trusted:

1. NovelAI says so - ``Source`` ("NovelAI Diffusion V4.5 4BDE2A90",
   "Stable Diffusion XL C1E1DE52" is V3), the request's ``model`` field, or a
   ComfyUI node that called the NovelAI API.
2. The checkpoint a local sampler actually ran.  A ComfyUI graph is followed
   from each sampler back up its ``model`` wire to the loader, so upscalers,
   face detectors, SeedVR and second-pass refiners do not count; the sampler
   that generated from noise (denoise 1) wins over a refiner.  The name then
   says the architecture: "waiIllustriousSDXL_v170", "anima-base-v1.0",
   "krea2_turbo_fp8".
3. When the name says nothing ("EMS-1641667-EMS", "turbo", "我是大模型"),
   what the site that made it recorded (Tensor.Art lists ``baseModel``), then
   the text encoder: Anima loads Qwen3 0.6B, Krea2 Qwen3-VL 4B, MOSS-Image
   Qwen3.5 9B, Flux.2 Klein Qwen3 8B - an encoder only fits one family.
4. A bundled SDXL-style checkpoint with no other evidence is SDXL if it
   generated at SDXL sizes; anything else that ran locally is 本地-未识别.

Pure functions, no I/O, standard library only: the archiver, the legacy
re-indexer and the tests import this file as it is.
"""

from __future__ import annotations

import json
import re

NAI_PREFIX = "NAI-"
NAI_UNKNOWN = "NAI-未知版本"
LOCAL_UNKNOWN = "本地-未识别"
UNKNOWN = "未知"

# Families in the order the gallery lists them inside each group.
NAI_FAMILIES = ("NAI-V5", "NAI-V4.5", "NAI-V4", "NAI-V3", "NAI-V1/V2", NAI_UNKNOWN)
LOCAL_FAMILIES = (
    "Illustrious", "NoobAI", "Pony", "SDXL", "SD1.5",
    "Anima", "Krea2", "MOSS-Image", "Qwen-Image", "Flux", "Chroma", "Z-Image",
    "NewBie", "Lumina", "HiDream", "HunyuanImage", "SD3", "Wan", LOCAL_UNKNOWN,
)
SDXL_FAMILIES = ("Illustrious", "NoobAI", "Pony", "SDXL")


def group_of(family: str | None) -> str:
    """The top-level group a family is listed under."""
    if family and family.startswith(NAI_PREFIX):
        return "NAI"
    if not family or family == UNKNOWN:
        return "其他"
    return "本地"


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------


def _loads(value):
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", "replace")
    if isinstance(value, str) and value.strip()[:1] in ("{", "["):
        try:
            return json.loads(value)
        except ValueError:
            return None
    return None


def _text(value) -> str:
    return value.strip() if isinstance(value, str) else ""


def basename(name: str) -> str:
    """``SDXL\\waiIllustriousSDXL_v170.safetensors`` -> ``waiIllustriousSDXL_v170.safetensors``."""
    return re.split(r"[\\/]", name.strip())[-1]


# --------------------------------------------------------------------------
# NovelAI
# --------------------------------------------------------------------------

_NAI_VERSION_PATTERNS = (
    # "NovelAI Diffusion V4.5 4BDE2A90", "NAI-Diffusion-v5-Full"
    re.compile(r"(?:novelai|nai)[\s_-]*diffusion[\s_-]*v(\d+(?:\.\d+)?)", re.I),
    # "DiffusionModelMetaName.NAIv5 9B720222", "NAIv4_5"
    re.compile(r"\bNAIv(\d+(?:[._]\d+)?)", re.I),
    # API ids: "nai-diffusion-4-5-full", "nai-diffusion-5-full-medium", "nai-diffusion-furry-3"
    re.compile(r"nai-diffusion-(?:furry-|anime-)?(\d+)(?:-(\d+))?(?=-|$)", re.I),
    # The ComfyUI NAI nodes' own menu: "V5 完整版", "V5 精选版", "V5 Full", "V4.5 Curated"
    re.compile(r"^\s*v(\d+(?:\.\d+)?)\s*(?:完整版|精选版|full|curated|furry|$)", re.I),
)


def _nai_version(text: str) -> str | None:
    """A NovelAI family from any of the ways a client names the model, or None."""
    text = _text(text)
    if not text:
        return None
    for pattern in _NAI_VERSION_PATTERNS:
        found = pattern.search(text)
        if found:
            major = found.group(1).replace("_", ".")
            minor = found.group(2) if pattern.groups >= 2 else None
            version = f"{major}.{minor}" if minor else major
            if version in ("5", "4.5", "4", "3"):
                return f"NAI-V{version}"
            if version in ("1", "2"):
                return "NAI-V1/V2"
            return None
    # Before V4 the Source was the base architecture plus a hash: V3 is the
    # SDXL one, V1/V2 the SD 1.x ones.
    if re.match(r"stable diffusion xl\b", text, re.I):
        return "NAI-V3"
    if re.match(r"stable diffusion [0-9A-F]{8}\b", text, re.I):
        return "NAI-V1/V2"
    return None


def _looks_like_nai_request(comment) -> bool:
    """NovelAI's request JSON, by the fields only it writes."""
    if not isinstance(comment, dict):
        return False
    markers = ("uc", "uncond_scale", "legacy_v3_extend", "v4_prompt", "signed_hash",
               "request_type", "noise_schedule", "sm_dyn", "cfg_rescale")
    if sum(1 for key in markers if key in comment) >= 2:
        return True
    sampler = comment.get("sampler")
    return "uc" in comment and isinstance(sampler, str) and sampler.startswith("k_")


def _novelai(fields: dict) -> tuple[str, str | None] | None:
    """(family, model) for a file NovelAI or a NovelAI client wrote, else None."""
    source = _text(fields.get("Source"))
    software = _text(fields.get("Software"))
    # PNG text chunks say "Comment"; a JPEG's comment segment reads as "comment".
    raw_comment = next((fields[k] for k in ("Comment", "comment", "UserComment")
                        if isinstance(fields.get(k), str) and fields[k].strip()), "")
    comment = _loads(raw_comment)
    if not isinstance(comment, dict) and _looks_like_nai_request(fields):
        comment = fields          # some clients write the request's keys directly
    is_nai = (
        bool(re.search(r"novelai|nai\b|nai-diffusion", software, re.I))
        or bool(_nai_version(source))
        or source.lower() in ("novelai", "image_edit_workspace")
        or _looks_like_nai_request(comment)
    )
    if not is_nai:
        return None
    candidates = [source]
    if isinstance(comment, dict):
        for key in ("model", "model_name", "modelName"):
            candidates.append(_text(comment.get(key)))
    for candidate in candidates:
        family = _nai_version(candidate)
        if family:
            return family, (source or candidate or None)
    # Some clients nest the model id deeper in the request (a passthrough or
    # director block); only the unambiguous spellings count there.
    nested = re.search(r"DiffusionModelMetaName\.NAIv\d+(?:_\d+)?|nai-diffusion-(?:furry-)?\d+(?:-\d+)?",
                       raw_comment if isinstance(raw_comment, str) else "")
    if nested and _nai_version(nested.group(0)):
        return _nai_version(nested.group(0)), (source or nested.group(0))
    return NAI_UNKNOWN, (source or None)


# --------------------------------------------------------------------------
# Local checkpoints: what the name says
# --------------------------------------------------------------------------

# Names of things a workflow loads that are not the image model.
_AUXILIARY = re.compile(
    r"seedvr|esrgan|ultrasharp|animesharp|remacri|swinir|swin2sr|\bhat\b|omnisr|nmkd|"
    r"realplksr|ani4k|janai|clearreality|nomos|superscale|upscale|"
    r"(?:^|[\\/_-])(?:sam|sam2|sam3)[._v-]|sam_vit|groundingdino|birefnet|rmbg|"
    r"yolo|bbox[\\/]|segm[\\/]|detector|depth[_-]?anything|midas|dwpose|openpose|sdpose|"
    r"controlnet|control_v\d|control-lora|ip[-_]?adapter|clip[_-]?vision|"
    r"tagger|wd-|pixai-tagger|cl_tagger|gfpgan|codeformer|"
    r"\.pth$|\.pt$|\.onnx$",
    re.I,
)

# Ordered: the first match wins, so a specific family comes before anything
# that would also match its name ("oneObsessionAnima" is Anima, not the
# Illustrious oneObsession; "waiANIMA" likewise).
_NAME_RULES: tuple[tuple[str, re.Pattern], ...] = tuple(
    (family, re.compile(pattern, re.I)) for family, pattern in (
        ("Krea2", r"krea[\s_-]?2"),
        ("MOSS-Image", r"moss[\s_-]?image|mossimage|(?:^|[\\/_\[])moss(?:[\\/_\]-]|融模)|pornmastermoss|haremmoss"),
        ("Anima", r"anima(?!gine|te|tion|l)|\bamina[-_]|qingwei|\[anima\]"),
        ("Qwen-Image", r"qwen[\s_-]?image|qwen[-_]rapid"),
        ("Flux", r"flux"),
        ("Chroma", r"chroma"),
        ("Z-Image", r"z[\s_-]?image"),
        ("NewBie", r"newbie"),
        ("HiDream", r"hidream"),
        ("HunyuanImage", r"hunyuan"),
        ("Lumina", r"lumina"),
        ("SD3", r"\bsd3|sd[\s_-]?3\.5|stable[\s_-]?diffusion[\s_-]?3"),
        ("Wan", r"(?:^|[\\/_-])wan[\s_-]?2"),
        # NoobAI and its descendants; v-prediction anime SDXL is NoobAI's.
        ("NoobAI", r"noob|nai[\s_-]?xl|naixl|ckxl|chenkin|v[\s_-]?pred|shadow(?:neo)?forge"),
        ("Pony", r"pony|pdxl|autismmix"),
        # The name says Illustrious ("illustrious", "-ill-", "_ilV3", "IL3"), or
        # it is a series published only as an Illustrious checkpoint.  Series
        # that also shipped SD 1.5 versions under the same name (Hassaku,
        # SweetMix, ntrMix) are left to the words in their name.
        ("Illustrious", r"illustrious|illu|(?:^|[^a-z])il(?:xl|v?\d)|_il[v\d]|"
                        r"(?:^|[^a-z])ill(?:[^a-z]|$)|"
                        r"wai[\s_-]?(?:nsfw|illu)|wainsfw|obsession|plant[\s_]?milk|rouwei|"
                        r"nova[a-z]{0,8}xl|ritual|miaomiao[\s_]?(?:20)?harem|angir"),
        ("Animagine", r"animagine"),   # SDXL; folded into SDXL below
        ("SD1.5", r"sd[\s_-]?1\.?5|sd15|v1-5|\bsd1\b"),
        ("SDXL", r"sdxl|\bxl\b|_xl|xl_|-xl"),
    )
)


def name_family(name: str | None) -> str | None:
    """The architecture a checkpoint's own name gives away, or None."""
    if not name:
        return None
    for family, pattern in _NAME_RULES:
        if pattern.search(name):
            return "SDXL" if family == "Animagine" else family
    return None


# Tensor.Art and similar sites label the base model themselves.
_SITE_BASES = (
    ("Krea2", r"krea[\s_]?2"),
    ("Anima", r"anima"),
    ("MOSS-Image", r"moss"),
    ("Qwen-Image", r"qwen"),
    ("NoobAI", r"noob"),
    ("Illustrious", r"illustrious"),
    ("Pony", r"pony"),
    ("Flux", r"flux"),
    ("Chroma", r"chroma"),
    ("Z-Image", r"z[\s_-]?image"),
    ("HiDream", r"hidream"),
    ("HunyuanImage", r"hunyuan"),
    ("SD3", r"sd[\s_]?3"),
    ("SD1.5", r"sd[\s_]?1(?:\.?5)?(?![0-9])"),
    ("SDXL", r"sdxl|xl"),
)


def site_family(base: str | None) -> str | None:
    if not base:
        return None
    for family, pattern in _SITE_BASES:
        if re.search(pattern, base, re.I):
            return family
    return None


# --------------------------------------------------------------------------
# Local checkpoints: what the rest of the workflow says
# --------------------------------------------------------------------------


def encoder_family(kind: str, name: str) -> str | None:
    """The family a text-encoder load implies (CLIPLoader ``type`` and file)."""
    kind = (kind or "").lower()
    name = basename(name or "").lower()
    if kind == "krea2" or re.search(r"qwen[_-]?3[_-]?vl[_-]?4b|qwen3vl4b|krea2text", name):
        return "Krea2"
    if kind == "moss_image" or "moss" in name:
        return "MOSS-Image"
    if kind == "flux2" or re.search(r"qwen[_-]?3[_-]?8b|qwen38b|fluxklein", name):
        return "Flux"
    if re.search(r"qwen[_-]?3[_-]?0?\.?6b|qwen306b|qwen3-0\.6b|qwen3508b|anima.*_txt", name):
        return "Anima"      # whatever ``type`` the user picked, only Anima loads this
    if kind == "qwen_image" or re.search(r"qwen[_-]?2\.?5[_-]?vl|qwen3vl[_-]?8b|qwen3\.5.*qwen_image", name):
        return "Qwen-Image"
    if kind == "chroma":
        return "Chroma"
    if kind == "hidream":
        return "HiDream"
    if kind in ("hunyuan_image", "hunyuan_image_2"):
        return "HunyuanImage"
    if kind == "sd3":
        return "SD3"
    if re.search(r"qwen[_-]?3[_-]?4b(?!.*vl)", name):
        return "Z-Image"
    if kind == "lumina2":
        return "NewBie" if "newbie" in name else "Lumina"
    if kind == "flux":
        return "Flux"
    return None


# Node classes that only exist for one family.  A loader among them is as good
# as a checkpoint name (MOSS-Image can be loaded straight from its Hugging Face
# repo, with no file name at all); any other is a weaker hint.
_FAMILY_LOADERS = (
    ("MOSS-Image", re.compile(r"^moss.*(?:loader|load)", re.I)),
    ("Anima", re.compile(r"^anima.*(?:loader|load)", re.I)),
    ("NewBie", re.compile(r"newbie.*load", re.I)),
)
_NODE_FAMILIES = (
    ("Anima", re.compile(r"^anima|anima(?:booster|pid|teacache|flow)", re.I)),
    ("Krea2", re.compile(r"krea2", re.I)),
    ("MOSS-Image", re.compile(r"^moss", re.I)),
    ("NewBie", re.compile(r"newbie", re.I)),
)

_NAI_NODES = re.compile(r"^NAI|NovelAI|NAID$|^GenerateNAID|^ModelOptionNAID", re.I)
_LOADER_CLASS = re.compile(r"load|checkpoint|unet|diffusionmodel", re.I)
_NOT_IMAGE_LOADER = re.compile(r"upscale|sam|detector|yolo|tagger|llm|llama|seedvr|controlnet|"
                               r"clip|vae|lora|ipadapter|style|bbox|segm|grounding|depth|"
                               r"birefnet|rmbg|image$|imageloader|loadimage", re.I)
_MODEL_KEYS = ("ckpt_name", "unet_name", "model_name", "diffusion_model", "base_ckpt_name")
_UPSTREAM_KEYS = ("model", "guider", "unet", "diffusion_model", "model_opt", "pipe",
                  "basic_pipe", "sdxl_tuple", "model1", "base_model")
_LATENT_KEYS = ("latent_image", "latent", "samples", "latents", "LATENT")
_MODEL_FILE = re.compile(r"\.(?:safetensors|ckpt|gguf|sft|pt|bin)$", re.I)
_SAMPLER = re.compile(r"sampler|ksampler|detailer|hiresfix", re.I)
_NOT_SAMPLER = re.compile(r"select|scheduler|sigma|NAI|NovelAI|sampler_name", re.I)


def _is_link(value) -> bool:
    return (isinstance(value, list) and len(value) == 2
            and isinstance(value[0], (str, int)) and isinstance(value[1], int))


def _linked_file(graph: dict, value) -> str | None:
    """A model file name typed into another node (a primitive, a selector) and wired in."""
    for _ in range(4):
        if not _is_link(value):
            return None
        node = graph.get(str(value[0]))
        if not isinstance(node, dict):
            return None
        inputs = node.get("inputs") or {}
        files = [v for v in inputs.values() if isinstance(v, str) and _MODEL_FILE.search(v.strip())]
        if files:
            return files[0].strip()
        value = next((v for v in inputs.values() if _is_link(v)), None)
    return None


def _node_model_name(node: dict, graph: dict | None = None) -> str | None:
    """The image model a loader node loads, if this node is one."""
    cls = str(node.get("class_type", ""))
    inputs = node.get("inputs") or {}
    for key in _MODEL_KEYS:
        value = inputs.get(key)
        if graph is not None and _is_link(value) and _LOADER_CLASS.search(cls):
            value = _linked_file(graph, value)
        if not isinstance(value, str) or not value.strip():
            continue
        if key == "model_name" and (not _LOADER_CLASS.search(cls) or _NOT_IMAGE_LOADER.search(cls)):
            continue
        if _AUXILIARY.search(value):
            continue
        return value.strip()
    return None


def _loader_family(cls: str) -> str | None:
    for family, pattern in _FAMILY_LOADERS:
        if pattern.search(cls):
            return family
    return None


def _sampler_nodes(graph: dict) -> list[tuple[str, dict]]:
    out = []
    for node_id, node in graph.items():
        if not isinstance(node, dict):
            continue
        cls = str(node.get("class_type", ""))
        inputs = node.get("inputs") or {}
        sampler_class = _SAMPLER.search(cls) and not _NOT_SAMPLER.search(cls)
        if sampler_class or ("steps" in inputs and any(k in inputs for k in ("model", "guider", "pipe"))):
            out.append((str(node_id), node))
    return out


def _denoise(node: dict) -> float:
    inputs = node.get("inputs") or {}
    value = inputs.get("denoise")
    if isinstance(value, (int, float)):
        return float(value)
    cls = str(node.get("class_type", ""))
    if re.search(r"detailer|upscale|hiresfix", cls, re.I):
        return 0.5
    return 1.0


def _latent_kind(cls: str) -> int | None:
    """0: an empty latent (text to image), 2: an encoded image, 3: another sampler."""
    if re.search(r"empty.*latent|latent.*empty", cls, re.I):
        return 0
    if re.search(r"vaeencode|encode.*latent|inpaint", cls, re.I):
        return 2
    if _SAMPLER.search(cls) and not _NOT_SAMPLER.search(cls):
        return 3
    return None


def _latent_origin(graph: dict, node: dict) -> int:
    """Where a sampler's starting latent comes from; 1 when it cannot be told."""
    for _ in range(10):
        inputs = node.get("inputs") or {}
        link = next((inputs[k] for k in _LATENT_KEYS if _is_link(inputs.get(k))), None)
        if link is None:
            return 1
        node = graph.get(str(link[0]))
        if not isinstance(node, dict):
            return 1
        kind = _latent_kind(str(node.get("class_type", "")))
        if kind is not None:
            return kind
    return 1


def _trace_loader(graph: dict, start: str) -> tuple[str | None, str | None]:
    """Walk up a sampler's model wire to the checkpoint it runs: (name, family loader)."""
    seen = set()
    frontier = [start]
    for _ in range(40):
        if not frontier:
            return None, None
        next_frontier = []
        for node_id in frontier:
            if node_id in seen:
                continue
            seen.add(node_id)
            node = graph.get(node_id)
            if not isinstance(node, dict):
                continue
            if node_id != start:
                name = _node_model_name(node, graph)
                if name:
                    return name, None
                family = _loader_family(str(node.get("class_type", "")))
                if family:
                    return None, family
            inputs = node.get("inputs") or {}
            found = [str(inputs[key][0]) for key in _UPSTREAM_KEYS if _is_link(inputs.get(key))]
            if node_id != start and not found:
                # Switch nodes and the like name their inputs freely.
                found = [str(value[0]) for key, value in inputs.items()
                         if _is_link(value) and re.search(r"model|input|any|on_true|on_false|select", str(key), re.I)]
            next_frontier += found
        frontier = next_frontier
    return None, None


class _Evidence:
    """Everything a workflow says about its model, before deciding."""

    def __init__(self):
        self.primary: str | None = None        # checkpoint the generating sampler ran
        self.primary_family: str | None = None  # ... or the family-only loader it ran
        self.loaded: list[str] = []            # every image-model name loaded
        self.loader_families: list[str] = []   # family-only loaders anywhere in the graph
        self.site_base: str | None = None      # e.g. Tensor.Art's baseModel
        self.site_name: str | None = None      # e.g. Tensor.Art's label for it
        self.encoders: list[str] = []          # families implied by text encoders
        self.nodes: list[str] = []             # families implied by other node classes
        self.nai: str | None = None            # a NovelAI family, from API nodes
        self.nai_model: str | None = None
        self.bundled = False                   # a checkpoint loader that brings its own CLIP
        self.latent_area = 0                   # largest empty latent, in pixels


def _encoder(ev: _Evidence, cls: str, kind: str, name: str) -> None:
    family = encoder_family(kind, name)
    if not family:
        family = _loader_family(cls) or ("Anima" if "Anima" in cls else None)
    if family:
        ev.encoders.append(family)


def _api_graph(graph: dict, ev: _Evidence) -> None:
    for node in graph.values():
        if not isinstance(node, dict):
            continue
        cls = str(node.get("class_type", ""))
        inputs = node.get("inputs") or {}
        if _NAI_NODES.search(cls):
            model = _text(inputs.get("model")) or _text(inputs.get("model_name"))
            family = _nai_version(model) if model else None
            if family:
                ev.nai, ev.nai_model = family, model
            elif re.search(r"sampler|generate|texttoimage", cls, re.I) and not ev.nai:
                ev.nai = NAI_UNKNOWN
        name = _node_model_name(node, graph)
        if name:
            ev.loaded.append(name)
            if "ckpt_name" in inputs and not re.search(r"unet|diffusion", cls, re.I):
                ev.bundled = True
        is_encoder = bool(re.search(r"cliploader|textencoderloader|clip_loader", cls, re.I))
        if not is_encoder and not name and _loader_family(cls):
            ev.loader_families.append(_loader_family(cls))
        for key in ("_base_model", "base_model_type"):
            if isinstance(inputs.get(key), str) and inputs[key].strip():
                ev.site_base = ev.site_base or inputs[key].strip()
        if isinstance(inputs.get("tams_ckpt_name"), str) and inputs["tams_ckpt_name"].strip():
            ev.site_name = ev.site_name or inputs["tams_ckpt_name"].strip()
        if is_encoder:
            kind = _text(inputs.get("type"))
            for key in ("clip_name", "clip_name1", "clip_name2", "clip_name3", "text_encoder"):
                if isinstance(inputs.get(key), str):
                    _encoder(ev, cls, kind, inputs[key])
        for family, pattern in _NODE_FAMILIES:
            if pattern.search(cls):
                ev.nodes.append(family)
        if re.search(r"emptylatent|emptysd3latent|emptyflux2latent", cls, re.I):
            width, height = inputs.get("width"), inputs.get("height")
            if isinstance(width, (int, float)) and isinstance(height, (int, float)):
                ev.latent_area = max(ev.latent_area, int(width) * int(height))
        sampler = _text(inputs.get("sampler_name"))
        if "sdxl" in sampler.lower():
            ev.nodes.append("SDXL")
        task = _text(inputs.get("task_type"))
        if task:
            family = site_family(task)
            if family:
                ev.nodes.append(family)

    # The generating sampler: the one that starts from an empty latent, then
    # one at full denoise, before any refiner or second pass.
    samplers = sorted(_sampler_nodes(graph), key=lambda item: (
        _latent_origin(graph, item[1]), _denoise(item[1]) < 0.99, _id_order(item[0])))
    for node_id, _node in samplers:
        name, family = _trace_loader(graph, node_id)
        if name or family:
            ev.primary, ev.primary_family = name, family
            break


def _id_order(node_id: str):
    head = node_id.split(":")[0]
    return (0, int(head)) if head.isdigit() else (1, node_id)


_UI_LOADER = re.compile(r"checkpointloader|unetloader|unet loader|diffusionmodelloader|"
                        r"mossimageunetloader|otunetloader|easy (?:full|comfy|a1111)loader|"
                        r"efficient loader|eff\. loader", re.I)
_UI_ENCODER = re.compile(r"cliploader", re.I)


_SUBGRAPH_INPUT = -10      # the id ComfyUI gives a subgraph's own input node


def _promoted(definition: dict, instance: dict | None) -> dict:
    """Subgraph input slot -> the value its instance supplies for it.

    A subgraph can expose an inner widget (a loader's file name) on the node
    that instantiates it; the inner widget then keeps a stale default and the
    instance holds what actually ran.
    """
    if not instance:
        return {}
    values = instance.get("widgets_values")
    if not isinstance(values, list):
        return {}
    slots = [slot.get("name") for slot in definition.get("inputs") or [] if isinstance(slot, dict)]
    widget_inputs = [slot.get("name") for slot in instance.get("inputs") or []
                     if isinstance(slot, dict) and slot.get("widget")]
    if widget_inputs and len(widget_inputs) == len(values):
        by_name = dict(zip(widget_inputs, values))
        return {index: by_name[name] for index, name in enumerate(slots) if name in by_name}
    if len(values) == len(slots):
        return dict(enumerate(values))
    return {}


def _ui_graphs(workflow: dict):
    """The saved UI workflow and every subgraph a live node instantiates.

    Yields (nodes by id, link id -> (origin id, origin slot), promoted values).
    Subgraphs whose every instance is muted or bypassed never ran and are
    skipped - a disabled inpainting group must not name the model.
    """
    definitions = {}
    pending = [workflow]
    while pending:
        current = pending.pop()
        for subgraph in (current.get("definitions") or {}).get("subgraphs") or []:
            if isinstance(subgraph, dict) and subgraph.get("id") not in definitions:
                definitions[subgraph.get("id")] = subgraph
                pending.append(subgraph)
    queue = [(workflow, None)]
    entered = set()
    while queue:
        current, instance = queue.pop(0)
        nodes = {node.get("id"): node for node in current.get("nodes") or []
                 if isinstance(node, dict) and "id" in node}
        links = {}
        for link in current.get("links") or []:
            if isinstance(link, list) and len(link) >= 3:
                links[link[0]] = (link[1], link[2])
            elif isinstance(link, dict) and "id" in link:
                links[link["id"]] = (link.get("origin_id"), link.get("origin_slot"))
        yield nodes, links, _promoted(current, instance)
        for node in nodes.values():
            kind = node.get("type")
            if kind in definitions and kind not in entered and node.get("mode", 0) not in (2, 4):
                entered.add(kind)
                queue.append((definitions[kind], node))


def _ui_input(node: dict, links: dict, nodes: dict, names) -> dict | None:
    """The node wired into one of ``names`` on this node, within the same graph."""
    for slot in node.get("inputs") or []:
        if isinstance(slot, dict) and slot.get("name") in names and slot.get("link") is not None:
            origin = links.get(slot["link"])
            return nodes.get(origin[0]) if origin else None
    return None


def _ui_strings(node: dict, links: dict | None = None, promoted: dict | None = None) -> list[str]:
    """A node's string widgets, with values its subgraph instance supplies swapped in."""
    values = node.get("widgets_values")
    values = list(values) if isinstance(values, list) else []
    if links is not None and promoted:
        widget_slots = [slot for slot in node.get("inputs") or [] if isinstance(slot, dict) and slot.get("widget")]
        positional = len(widget_slots) == len(values)
        for index, slot in enumerate(widget_slots):
            origin = links.get(slot.get("link")) if slot.get("link") is not None else None
            if not origin or origin[0] != _SUBGRAPH_INPUT or origin[1] not in promoted:
                continue
            # Older saves list only the widgets turned into inputs; the
            # loaders' own widgets are in a known order.
            target = index if positional else _WIDGET_INDEX.get(slot.get("name"))
            if target is not None and target < len(values):
                values[target] = promoted[origin[1]]
    return [v.strip() for v in values if isinstance(v, str) and v.strip()]


_WIDGET_INDEX = {"ckpt_name": 0, "unet_name": 0, "clip_name": 0, "vae_name": 0, "model_name": 0, "type": 1}


def _ui_trace(node: dict, nodes: dict, links: dict, promoted: dict) -> tuple[str | None, str | None]:
    for _ in range(30):
        node = _ui_input(node, links, nodes, ("model", "guider", "MODEL", "unet", "pipe"))
        if node is None:
            return None, None
        kind = str(node.get("type", ""))
        strings = _ui_strings(node, links, promoted)
        if _UI_LOADER.search(kind) and strings and not _AUXILIARY.search(strings[0]):
            return strings[0], None
        if _loader_family(kind) and not _UI_ENCODER.search(kind):
            return None, _loader_family(kind)
    return None, None


def _ui_latent_origin(node: dict, nodes: dict, links: dict) -> int:
    for _ in range(10):
        node = _ui_input(node, links, nodes, _LATENT_KEYS)
        if node is None:
            return 1
        kind = _latent_kind(str(node.get("type", "")))
        if kind is not None:
            return kind
    return 1


def _ui_denoise(node: dict) -> float:
    kind = str(node.get("type", ""))
    values = node.get("widgets_values")
    if kind == "KSampler" and isinstance(values, list) and len(values) >= 7 and isinstance(values[6], (int, float)):
        return float(values[6])
    return 1.0


def _ui_workflow(workflow: dict, ev: _Evidence) -> None:
    """The editor's saved workflow: widgets by position, wires by link id.

    It records how the graph was left, which can differ from what ran, so it
    only names the checkpoint when nothing more direct (an API graph, the
    parameters a save node wrote) already did; its encoders and nodes still
    count as evidence.
    """
    samplers = []
    for nodes, links, promoted in _ui_graphs(workflow):
        for node in nodes.values():
            if node.get("mode", 0) in (2, 4):      # muted or bypassed
                continue
            kind = str(node.get("type", ""))
            values = node.get("widgets_values")
            values = values if isinstance(values, list) else []
            strings = _ui_strings(node, links, promoted)
            if _NAI_NODES.search(kind):
                for value in strings:
                    family = _nai_version(value)
                    if family:
                        ev.nai, ev.nai_model = family, value
                        break
            if _UI_LOADER.search(kind) and strings and not _AUXILIARY.search(strings[0]):
                ev.loaded.append(strings[0])
                if "checkpoint" in kind.lower():
                    ev.bundled = True
            elif _UI_ENCODER.search(kind) and strings:
                _encoder(ev, kind, strings[1] if len(strings) > 1 else "", strings[0])
            elif _loader_family(kind):
                ev.loader_families.append(_loader_family(kind))
            for family, pattern in _NODE_FAMILIES:
                if pattern.search(kind):
                    ev.nodes.append(family)
            if re.search(r"emptylatent", kind, re.I) and len(values) >= 2:
                width, height = values[0], values[1]
                if isinstance(width, (int, float)) and isinstance(height, (int, float)):
                    ev.latent_area = max(ev.latent_area, int(width) * int(height))
            if _SAMPLER.search(kind) and not _NOT_SAMPLER.search(kind):
                samplers.append((_ui_latent_origin(node, nodes, links), _ui_denoise(node) < 0.99,
                                 len(samplers), node, nodes, links, promoted))
    if ev.primary or ev.primary_family:
        return
    for *_rank, node, nodes, links, promoted in sorted(samplers, key=lambda item: item[:3]):
        name, family = _ui_trace(node, nodes, links, promoted)
        if name or family:
            ev.primary, ev.primary_family = name, family
            return
    if ev.loaded:
        ev.primary = ev.loaded[0]


def _site_data(fields: dict, ev: _Evidence) -> None:
    """Tensor.Art's ``generation_data`` names the checkpoint and its base."""
    data = _loads(fields.get("generation_data"))
    if not isinstance(data, dict):
        return
    models = data.get("models") if isinstance(data.get("models"), list) else []
    checkpoint = next((m for m in models if isinstance(m, dict)
                       and str(m.get("type", "")).upper() in ("CHECKPOINT", "UNET", "BASE_MODEL")), None)
    if checkpoint:
        ev.site_name = ev.site_name or _text(checkpoint.get("label")) or _text(checkpoint.get("modelFileName")) or None
        ev.site_base = ev.site_base or _text(checkpoint.get("baseModel")) or None
    if not ev.site_base:
        # Every LoRA has to match the base it is applied to.
        for model in models:
            if isinstance(model, dict) and _text(model.get("baseModel")):
                ev.site_base = _text(model.get("baseModel"))
                break
    if not ev.site_base and _text(data.get("baseModel")):
        ev.site_base = _text(data.get("baseModel"))


# A1111 writes one ``key: value`` list; ControlNet units nest their own
# "Model:" inside a quoted value, which must not replace the checkpoint.
_A1111_KEYS = re.compile(r'(?:^|,\s*|\n)(Model|Model hash|Size|Module 1|Sampler|Version):\s*("(?:[^"\\]|\\.)*"|[^,\n]*)')


_AIR = re.compile(r"urn:air:([a-z0-9._-]+):(checkpoint|lora|lycoris|locon|embedding|dora)\b", re.I)


def _a1111(text: str, ev: _Evidence, name_model: bool = True) -> None:
    """A1111's parameter text, or a ComfyUI save node writing the same format.

    Written when the image was saved, so its ``Model`` is what ran.  Civitai's
    resource list names each resource's ecosystem ("urn:air:krea2:lora:..."),
    and a LoRA only loads on the base it was trained for.
    """
    tail_start = text.rfind("\nSteps:")
    tail = text[tail_start:] if tail_start >= 0 else text
    airs = _AIR.findall(tail)
    checkpoints = [eco for eco, kind in airs if kind.lower() == "checkpoint"]
    ecosystems = checkpoints or [eco for eco, _kind in airs]
    if ecosystems and not ev.site_base:
        ev.site_base = _majority(ecosystems)
    # Remove quoted values (ControlNet, ADetailer prompts) before reading keys.
    stripped = re.sub(r'"(?:[^"\\]|\\.)*"', '""', tail)
    found = {}
    for key, value in _A1111_KEYS.findall(stripped):
        found.setdefault(key, value.strip())
    model = found.get("Model")
    if name_model and model and not _AUXILIARY.search(model) and model.lower() != "none":
        ev.primary = model
        ev.loaded.append(model)
        ev.bundled = ev.bundled or "comfyui" not in found.get("Version", "").lower()
    size = re.match(r"(\d+)\s*x\s*(\d+)", found.get("Size", ""))
    if size:
        ev.latent_area = int(size.group(1)) * int(size.group(2))
    module = found.get("Module 1", "")
    if module:
        family = encoder_family("", module)
        if family:
            ev.encoders.append(family)
    if "sdxl" in found.get("Sampler", "").lower():
        ev.nodes.append("SDXL")


def _decide(ev: _Evidence) -> tuple[str, str | None]:
    if ev.nai:
        return ev.nai, ev.nai_model
    name = ev.primary or (ev.loaded[0] if ev.loaded and not ev.primary_family else None)
    # Tensor.Art stores checkpoints as "EMS-<id>-EMS"; its own label is the name.
    ems = re.match(r"EMS-(\d+)-EMS", basename(name)) if name else None
    display = ev.site_name if (not name or ems) else None
    if ems and not display:
        display = f"Tensor.Art #{ems.group(1)}"
    by_name = name_family(name) or name_family(display)
    by_loader = ev.primary_family or _majority(ev.loader_families)
    by_site = site_family(ev.site_base)
    by_encoder = _majority(ev.encoders)
    by_nodes = _majority(ev.nodes)

    family = by_name
    # A name that only hints at SDXL loses to evidence that cannot be SDXL:
    # an all-in-one Anima checkpoint loaded with CheckpointLoaderSimple still
    # needs Anima's text encoder.
    other = by_loader or by_encoder
    if family in SDXL_FAMILIES and other and other not in SDXL_FAMILIES and _weak_sdxl_name(name):
        family = other
    if not family and ev.primary_family:
        family = ev.primary_family
    family = family or by_site or by_loader or by_encoder or by_nodes
    if not family and name and ev.bundled:
        if ev.latent_area >= 700_000:
            family = "SDXL"
        elif 0 < ev.latent_area <= 450_000:
            family = "SD1.5"
    if not family:
        family = LOCAL_UNKNOWN
    return family, display_name(display or name, family)


def _weak_sdxl_name(name: str | None) -> bool:
    """True when only a curated list, not the name itself, said SDXL."""
    if not name:
        return True
    return not re.search(r"illustrious|noob|pony|sdxl|naixl|nai[_-]xl|vpred|v-pred", name, re.I)


def _majority(families: list[str]) -> str | None:
    if not families:
        return None
    counts = {}
    for family in families:
        counts[family] = counts.get(family, 0) + 1
    return max(counts, key=lambda f: (counts[f], -families.index(f)))


# --------------------------------------------------------------------------
# Display names
# --------------------------------------------------------------------------

# Well-known bases whose files circulate under many names and quantisations.
_CANONICAL = tuple((re.compile(pattern, re.I), shown) for pattern, shown in (
    (r"^anima[-_ ]?base[-_ ]?(?:202605[-_])?v?1\.?0|^animabasev10|^anima_base-v1\.0", "Anima Base v1.0"),
    (r"^anima[-_ ]?preview[-_ ]?3|^anima_preview3", "Anima Preview 3"),
    (r"^anima[-_ ]?preview", "Anima Preview"),
    (r"^anima[-_ ]?aesthetic[-_ ]?v?1\.?1", "Anima Aesthetic v1.1"),
    (r"^anima[-_ ]?aesthetic[-_ ]?v?1\.?0", "Anima Aesthetic v1.0"),
    (r"^anima[-_ ]?turbo[-_ ]?v?1\.?1", "Anima Turbo v1.1"),
    (r"^anima[-_ ]?turbo[-_ ]?v?1\.?0", "Anima Turbo v1.0"),
    (r"^anima[-_ ]?2\.?9b[-_ ]?preview", "Anima 2.9B Preview"),
    (r"^anima[-_ ]?2\.?9b|^anima29b", "Anima 2.9B"),
    (r"^krea2[-_ ]?turbo[-_ ]?anime", "Krea2 Turbo Anime"),
    (r"^krea2[-_ ]?turbo(?!.*(?:nsfw|uncensored))|^krea2turboofficialcomfy|^krea2turbofp8", "Krea2 Turbo"),
    (r"^moss[-_ ]?image[-_ ]?sft[-_ ]?turbo", "MOSS-Image SFT Turbo"),
    (r"^moss[-_ ]?image[-_ ]?(?:sft|_sft)|^mossimage_sft|^moss-image-sft", "MOSS-Image SFT"),
    (r"^moss[-_ ]?image[-_ ]?heal", "MOSS-Image Heal"),
    (r"^moss[-_ ]?image[-_ ]?base|^mossimage_base|^moss-image-base", "MOSS-Image Base"),
    (r"^qwen[-_ ]?image[-_ ]?edit[-_ ]?(\d+)", "Qwen-Image Edit"),
    (r"^qwen[-_ ]?image[-_ ]?2\.?1", "Qwen-Image 2.1"),
    (r"^flux[-_ .]?2[-_ ]?klein[-_ ]?9b(?:[-_ ]?(?:kv|fp8|bf16|q\d)|$)", "Flux.2 Klein 9B"),
    (r"^flux[-_ .]?2[-_ ]?klein[-_ ]?4b(?:[-_ ]?(?:kv|fp8|bf16|q\d)|$)", "Flux.2 Klein 4B"),
    (r"^animagine[-_ ]?xl[-_ ]?4", "Animagine XL 4.0"),
))

_WAI = re.compile(r"wai[-_ ]?(?:nsfw[-_ ]?)?illustrious(?:[-_ ]?sdxl)?[-_ ]?v?(\d{2,3})|"
                  r"wai[-_ ]?nsfw[-_ ]?illustrious[-_ ]?(\d{2,3})", re.I)

_QUANT_SUFFIX = re.compile(
    r"(?:[-_. ](?:fp8(?:_e4m3fn)?(?:_scaled)?|fp16|bf16|fp32|int8(?:[-_]?convrot)?(?:[-_]?attn)?|int8c|int8mixed|"
    r"convrot(?:[-_]?int8mixed)?|nvfp4|mxfp8(?:mixed)?|fp8mixed|w4a4|w4a8(?:_gptq)?|q\d_k(?:_[ms])?|q\d_0|"
    r"pruned|scaled|full|\d{6,8})|\.[A-Za-z0-9]{4})+$", re.I)


def display_name(name: str | None, family: str | None = None) -> str | None:
    """A checkpoint as people would name it: no folders, extension or quantisation."""
    if not name:
        return None
    stem = basename(name)
    stem = re.sub(r"\.(safetensors|ckpt|gguf|pt|pth|bin|sft)$", "", stem, flags=re.I)
    stem = re.sub(r"\.(safetensors|ckpt|gguf)$", "", stem, flags=re.I)  # "x.safetensors.safetensors"
    stem = re.sub(r"\s*\(\d+\)$", "", stem).strip(" .")
    stem = re.sub(r"\s*\[[0-9a-f]{8,10}\]$", "", stem, flags=re.I)      # A1111 "[hash]"
    for pattern, shown in _CANONICAL:
        if pattern.search(stem):
            return shown
    wai = _WAI.search(stem)
    if wai:
        version = wai.group(1) or wai.group(2)
        version = version[:-1] if len(version) == 3 and version.endswith("0") else version
        return f"WAI-Illustrious v{version}"
    # Strip quantisation and checksum tails ("_int8_convrot", Civitai's
    # ".lCmh" rename, a "_1390979" version id), but keep something.
    trimmed = _QUANT_SUFFIX.sub("", stem).strip(" -_.") or stem
    if len(trimmed) < 4:
        trimmed = stem
    return trimmed[:120]


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------


def classify(source: str | None, fields, category: str | None = None) -> tuple[str, str | None]:
    """(family, model) for one image's parsed metadata.

    ``source`` is the collector's ``metadata_source``; ``fields`` is the parsed
    ``metadata_json`` (the PNG text chunks, EXIF strings and so on, as a dict).
    """
    fields = _loads(fields) if not isinstance(fields, dict) else fields
    if not isinstance(fields, dict):
        fields = {}
    source = source or ""

    nai = _novelai(fields)
    if nai and not (source in ("comfyui",) and _loads(fields.get("prompt"))):
        return nai

    # Most direct first: the executed API graph, then the parameters a save
    # node wrote, then the editor's workflow, which can be stale.
    ev = _Evidence()
    graph = _loads(fields.get("prompt"))
    if isinstance(graph, dict) and any(isinstance(v, dict) and "class_type" in v for v in graph.values()):
        _api_graph(graph, ev)
    _site_data(fields, ev)
    parameters = fields.get("parameters")
    if not isinstance(parameters, str):
        user_comment = fields.get("UserComment")
        parameters = user_comment if isinstance(user_comment, str) and "Steps:" in user_comment else None
    if isinstance(parameters, str) and not ev.nai:
        _a1111(parameters, ev, name_model=not (ev.primary or ev.primary_family))
    workflow = _loads(fields.get("workflow"))
    if isinstance(workflow, dict) and not ev.nai and not (isinstance(graph, dict) and ev.primary):
        _ui_workflow(workflow, ev)
    extra = _loads(fields.get("extra"))
    if isinstance(extra, dict):
        family = site_family(_text(extra.get("task_type")))
        if family:
            ev.nodes.append(family)

    if nai and not ev.primary and not ev.nai:
        return nai
    if (ev.nai or ev.primary or ev.primary_family or ev.loaded or ev.loader_families
            or ev.site_base or ev.site_name or ev.encoders or ev.nodes):
        return _decide(ev)
    if source in ("comfyui", "a1111-compatible", "unknown-generator") and (graph or workflow or parameters):
        return LOCAL_UNKNOWN, None
    if (category or "").startswith("NAI") or source.startswith("novelai"):
        return NAI_UNKNOWN, None
    return UNKNOWN, None
