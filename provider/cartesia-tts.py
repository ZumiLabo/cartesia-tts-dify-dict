import logging
from collections.abc import Mapping

from cartesia import Cartesia
from dify_plugin import ModelProvider
from dify_plugin.errors.model import CredentialsValidateFailedError

logger = logging.getLogger(__name__)


class CartesiaTtsModelProvider(ModelProvider):
    def _extract_voice_ids(self, voices_response) -> set[str]:
        voice_ids: set[str] = set()

        if voices_response is None:
            return voice_ids

        if isinstance(voices_response, list):
            for item in voices_response:
                voice_id = getattr(item, "id", None)
                if voice_id:
                    voice_ids.add(voice_id)
            return voice_ids

        data = getattr(voices_response, "data", None)
        if isinstance(data, list):
            for item in data:
                voice_id = getattr(item, "id", None)
                if voice_id:
                    voice_ids.add(voice_id)
            return voice_ids

        try:
            for item in voices_response:
                voice_id = getattr(item, "id", None)
                if voice_id:
                    voice_ids.add(voice_id)
        except TypeError:
            pass

        return voice_ids

    def validate_provider_credentials(self, credentials: Mapping) -> None:
        api_key = credentials.get("cartesia_api_key")
        voice_id = credentials.get("voice_id")

        if not api_key or not voice_id:
            raise CredentialsValidateFailedError(
                "Missing required credentials: 'cartesia_api_key' and 'voice_id'."
            )

        try:
            client = Cartesia(api_key=api_key)
            voices_response = client.voices.list(limit=100)
            voice_ids = self._extract_voice_ids(voices_response)

            if voice_ids and voice_id not in voice_ids:
                raise CredentialsValidateFailedError(
                    f"Voice ID '{voice_id}' not found in the available voices."
                )
        except CredentialsValidateFailedError:
            raise
        except Exception as ex:
            logger.exception("cartesia-tts credentials validate failed")
            raise CredentialsValidateFailedError(str(ex))
