from datetime import datetime
from typing import Dict

from pydantic import BaseModel, ConfigDict


class Theta(BaseModel):
    __pydantic_extra__: Dict[str, float]
    time_secs: datetime
    model_config = ConfigDict(extra="allow")
