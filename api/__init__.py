import abc
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Union


@dataclass
class User:
    name: str
    login: str
    id: int = 0
    profile_image_url: str = ""
    offline_image_url: str = ""
    broadcaster_type: str = ""
    description: str = ""
    type: str = ""
    def __hash__(self):
        return hash(self.id)
    def __eq__(self, other):
        return self.id == other.id


@dataclass
class Game:
    id: int
    name: str
    box_art_url: str
    def __hash__(self):
        return hash(self.id)
    def __eq__(self, other):
        return self.id == other.id

@dataclass
class Chat:
    user_id: str
    chat: str
    name: str = None
    is_subscriber: bool = False
    subscribe_month: int = 0
    is_follower: bool = False
    is_female: bool = None
    is_fanclub: bool = None
    is_topfan: bool = None


@dataclass
class Stream:
    id: int
    game: Game
    user: User
    language: str
    title: str
    started_at: datetime
    viewer_count: int = 0
    chatter_count: int = 0
    chatters: List[Union[str, object]] = field(default_factory=list)
    chattings: List[Chat] = field(default_factory=list)
    follower_count: int = 0
    main_viewer_count: int = 0
    sub_viewer_count: int = 0
    join_viewer_count: int = 0
    def __hash__(self):
        return hash(self.id)
    def __eq__(self, other):
        return self.id == other.id
    def metadata_eq(self, other):
        if not other:
            return False
        return ((self.game is None and other.game is None) or (self.game and other.game and self.game.id == other.game.id) and self.title == other.title and self.started_at == other.started_at)
    def get_chatter_count(self):
        if self.chatter_count:
            return self.chatter_count
        else:
            return len(self.chatters)

class API(abc.ABC):
    @abc.abstractmethod
    async def streams(self) -> List[Stream]: pass
