from __future__ import annotations

import concurrent.futures
import re
from collections.abc import Generator
from typing import Optional

from cartesia import Cartesia
from dify_plugin import TTSModel
from dify_plugin.errors.model import (
    CredentialsValidateFailedError,
    InvokeBadRequestError,
    InvokeError,
    InvokeServerUnavailableError,
)


class CartesiaTtsText2SpeechModel(TTSModel):
    """Cartesia TTS model with pronunciation dictionary support."""

    DEFAULT_MODEL_ID = "sonic-3"
    DEFAULT_LANGUAGE = "ja"

    def _resolve_model_id(self, model: str | None) -> str:
        return model or self.DEFAULT_MODEL_ID

    def _make_payload(
        self,
        *,
        model_id: str,
        transcript: str,
        voice_id: str,
        pronunciation_dict_id: str | None,
        sample_rate: int,
    ) -> dict:
        payload = {
            "model_id": model_id,
            "transcript": transcript,
            "voice": {"id": voice_id},
            "language": self.DEFAULT_LANGUAGE,
            "output_format": {
                "container": "mp3",
                "bit_rate": 192000,
                "sample_rate": sample_rate,
            },
        }
        if pronunciation_dict_id:
            payload["pronunciation_dict_id"] = pronunciation_dict_id
        return payload

    def _invoke(
        self,
        model: str,
        tenant_id,
        credentials: dict,
        content_text: str,
        voice: str,
        user: Optional[str] = None,
    ) -> bytes | Generator[bytes, None, None]:
        api_key = credentials.get("cartesia_api_key")
        voice_id = credentials.get("voice_id")
        pronunciation_dict_id = credentials.get("pronunciation_dict_id")
        model_id = self._resolve_model_id(model)

        client = Cartesia(api_key=api_key)
        payload = self._make_payload(
            model_id=model_id,
            transcript=content_text,
            voice_id=voice_id,
            pronunciation_dict_id=pronunciation_dict_id,
            sample_rate=44100,
        )
        return client.tts.bytes(**payload)

    def _tts_invoke(self, model: str, credentials: dict, content_text: str, voice: str):
        api_key = credentials.get("cartesia_api_key")
        voice_id = credentials.get("voice_id")
        pronunciation_dict_id = credentials.get("pronunciation_dict_id")
        model_id = self._resolve_model_id(model)
        max_workers = self._get_model_workers_limit(model, credentials)

        try:
            sentences = self._split_sentences(content_text)
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(
                        self._process_sentence,
                        sentence=sentence,
                        api_key=api_key,
                        voice_id=voice_id,
                        pronunciation_dict_id=pronunciation_dict_id,
                        model_id=model_id,
                    )
                    for sentence in sentences
                    if sentence.strip()
                ]
                for future in futures:
                    yield future.result()
        except Exception as ex:
            raise InvokeBadRequestError(str(ex))

    def _process_sentence(
        self,
        sentence: str,
        api_key: str,
        voice_id: str,
        pronunciation_dict_id: str | None = None,
        model_id: str | None = None,
    ):
        client = Cartesia(api_key=api_key)
        payload = self._make_payload(
            model_id=self._resolve_model_id(model_id),
            transcript=sentence,
            voice_id=voice_id,
            pronunciation_dict_id=pronunciation_dict_id,
            sample_rate=48000,
        )
        return client.tts.bytes(**payload)

    def _split_sentences(self, text: str) -> list[str]:
        normalized = text.replace("\r\n", "\n").replace("\r", "\n")
        parts = re.split(r"(?<=[。！？!?\n])", normalized)
        return [part.strip() for part in parts if part and part.strip()]

    def validate_credentials(
        self,
        model: str,
        credentials: dict,
        user: Optional[str] = None,
    ) -> None:
        api_key = credentials.get("cartesia_api_key")
        voice_id = credentials.get("voice_id")

        if not api_key or not voice_id:
            raise CredentialsValidateFailedError(
                "Missing required credentials: 'cartesia_api_key' and 'voice_id'."
            )

        try:
            client = Cartesia(api_key=api_key)
            voices = client.voices.list(limit=100)
            if not any(voice.id == voice_id for voice in voices):
                raise CredentialsValidateFailedError(
                    f"Voice ID '{voice_id}' not found in the available voices."
                )
        except CredentialsValidateFailedError:
            raise
        except Exception as ex:
            raise CredentialsValidateFailedError(str(ex))

    @property
    def _invoke_error_mapping(self) -> dict[type[InvokeError], list[type[Exception]]]:
        return {
            InvokeServerUnavailableError: [Exception],
        }
