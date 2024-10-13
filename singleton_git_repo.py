from git import Repo
import app_logger

logger = app_logger.get_logger(__name__)

class GitRepo():
    __instance: Repo = None

    def __init__(self, repo_dir: str):
        if not GitRepo.__instance:
            GitRepo.__instance = Repo(repo_dir)
        else:
            raise Exception('You cannot create another Git repo instance')

    @staticmethod
    def get_instance(repo_dir: str) -> Repo:
        if not GitRepo.__instance:
            GitRepo(repo_dir)
            return GitRepo.__instance