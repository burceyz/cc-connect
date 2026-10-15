#!/usr/bin/env python3
"""Subscription routing configuration and profile availability gates."""
import json
from pathlib import Path

ROUTING_CONFIG = 'routing.json'
DEFAULT_PROFILE = 'opencode-go'
VALID_PROFILES = ('opencode-go', 'happy2', 'happy1')


def load_routing(runtime):
    """Load and validate routing.json from runtime directory.

    Missing routing.json maintains compatibility:
        {'default_profile': 'opencode-go', 'disabled_profiles': []}
    Malformed file fails closed with ValueError('invalid_routing_config').
    """
    path = Path(runtime) / ROUTING_CONFIG
    if path.is_symlink():
        raise ValueError('invalid_routing_config')
    if not path.exists():
        return {
            'default_profile': DEFAULT_PROFILE,
            'disabled_profiles': [],
        }
    if not path.is_file():
        raise ValueError('invalid_routing_config')
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
    except Exception as err:
        raise ValueError('invalid_routing_config') from err

    if not isinstance(data, dict):
        raise ValueError('invalid_routing_config')
    if set(data.keys()) - {'default_profile', 'disabled_profiles'}:
        raise ValueError('invalid_routing_config')

    default_profile = data.get('default_profile')
    if not isinstance(default_profile, str) or default_profile not in VALID_PROFILES:
        raise ValueError('invalid_routing_config')

    if 'disabled_profiles' in data:
        disabled = data['disabled_profiles']
        if not isinstance(disabled, list) or any(
            not isinstance(p, str) or p not in VALID_PROFILES for p in disabled
        ):
            raise ValueError('invalid_routing_config')
        disabled_profiles = list(dict.fromkeys(disabled))
    else:
        disabled_profiles = []

    if default_profile in disabled_profiles:
        raise ValueError('invalid_routing_config')

    return {
        'default_profile': default_profile,
        'disabled_profiles': disabled_profiles,
    }


def check_profile_enabled(profile, runtime):
    """Raise ValueError('profile_disabled') if profile is disabled by routing."""
    config = load_routing(runtime)
    if profile in config['disabled_profiles']:
        raise ValueError('profile_disabled')
    return profile


def resolve_default_profile(runtime, tool_policy=None):
    """Return default profile for a new task.

    If tool_policy is 'none' or 'research', returns 'opencode-go' (fixed Go entrance).
    Otherwise returns config['default_profile'].
    """
    if tool_policy in ('none', 'research'):
        return DEFAULT_PROFILE
    config = load_routing(runtime)
    return config['default_profile']
