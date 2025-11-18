from asyncio import Task
from hashlib import md5
from json import dumps
from string import Template
from typing import Awaitable, Dict, Callable, Optional, List, Tuple

from httpx import AsyncClient
from yaml import safe_load

from manager_environment import EnvironmentManager as EM
from manager_debug import DebugManager as DBM

GITHUB_API_QUERIES = {
    "repos_contributed_to": """
{
    user(login: "$username") {
        repositoriesContributedTo(orderBy: {field: CREATED_AT, direction: DESC}, $pagination, includeUserRepositories: true) {
            nodes {
                primaryLanguage { name }
                name
                owner { login }
                isPrivate
                isFork
            }
            pageInfo { endCursor hasNextPage }
        }
    }
}""",
    "user_repository_list": """
{
    user(login: "$username") {
        repositories(orderBy: {field: CREATED_AT, direction: DESC}, $pagination, affiliations: [OWNER, COLLABORATOR], isFork: false) {
            nodes {
                primaryLanguage { name }
                name
                owner { login }
                isPrivate
            }
            pageInfo { endCursor hasNextPage }
        }
    }
}
""",
    "repo_branch_list": """
{
    repository(owner: "$owner", name: "$name") {
        refs(refPrefix: "refs/heads/", orderBy: {direction: DESC, field: TAG_COMMIT_DATE}, $pagination) {
            nodes { name }
            pageInfo { endCursor hasNextPage }
        }
    }
}
""",
    "repo_commit_list": """
{
    repository(owner: "$owner", name: "$name") {
        ref(qualifiedName: "refs/heads/$branch") {
            target {
                ... on Commit {
                    history(author: { id: "$id" }, $pagination) {
                        nodes {
                            ... on Commit { additions deletions committedDate oid }
                        }
                        pageInfo { endCursor hasNextPage }
                    }
                }
            }
        }
    }
}
""",
    "hide_outdated_comment": """
mutation {
    minimizeComment(input: {classifier: OUTDATED, subjectId: "$id"}) {
        clientMutationId
    }
}
""",
}


async def init_download_manager(user_login: str):
    await DownloadManager.load_remote_resources(
        linguist="https://cdn.jsdelivr.net/gh/github/linguist@master/lib/linguist/languages.yml",
        # 统一使用兼容端点，保证结构与 WakaTime v1 一致性更好
        waka_latest=f"http://wakapi.陈莹.我爱你/api/compat/wakatime/v1/users/current/stats/last_7_days?api_key={EM.WAKATIME_API_KEY}",
        waka_all=f"http://wakapi.陈莹.我爱你/api/compat/wakatime/v1/users/current/all_time_since_today?api_key={EM.WAKATIME_API_KEY}",
        github_stats=f"https://github-contributions.vercel.app/api/v1/{user_login}",
    )


class DownloadManager:
    _client = AsyncClient(timeout=60.0)
    _REMOTE_RESOURCES_CACHE = dict()

    @staticmethod
    async def load_remote_resources(**resources: str):
        for resource, url in resources.items():
            DownloadManager._REMOTE_RESOURCES_CACHE[resource] = DownloadManager._client.get(url)

    @staticmethod
    async def close_remote_resources():
        for resource in DownloadManager._REMOTE_RESOURCES_CACHE.values():
            if isinstance(resource, Task):
                resource.cancel()
            elif isinstance(resource, Awaitable):
                await resource

    @staticmethod
    async def _get_remote_resource(resource: str, convertor: Optional[Callable[[bytes], Dict]]) -> Dict or None:
        DBM.i(f"\tMaking a remote API query named '{resource}'...")
        if isinstance(DownloadManager._REMOTE_RESOURCES_CACHE[resource], Awaitable):
            res = await DownloadManager._REMOTE_RESOURCES_CACHE[resource]
            DownloadManager._REMOTE_RESOURCES_CACHE[resource] = res
            DBM.g(f"\tQuery '{resource}' finished, result saved!")
        else:
            res = DownloadManager._REMOTE_RESOURCES_CACHE[resource]
            DBM.g(f"\tQuery '{resource}' loaded from cache!")
        if res.status_code == 200:
            return res.json() if convertor is None else convertor(res.content)
        elif res.status_code in (201, 202):
            DBM.w(f"\tQuery '{resource}' returned {res.status_code} status code")
            return None
        else:
            raise Exception(f"Query '{res.url}' failed to run by returning code of {res.status_code}: {res.json()}")

    @staticmethod
    async def get_remote_json(resource: str) -> Dict or None:
        return await DownloadManager._get_remote_resource(resource, None)

    @staticmethod
    async def get_remote_yaml(resource: str) -> Dict or None:
        return await DownloadManager._get_remote_resource(resource, safe_load)

    @staticmethod
    async def _fetch_graphql_query(query: str, retries_count: int = 10, **kwargs) -> Dict:
        headers = {"Authorization": f"Bearer {EM.GH_TOKEN}"}
        res = await DownloadManager._client.post(
            "https://api.github.com/graphql",
            json={"query": Template(GITHUB_API_QUERIES[query]).substitute(kwargs)},
            headers=headers,
        )
        if res.status_code == 200:
            return res.json()
        elif res.status_code == 502 and retries_count > 0:
            return await DownloadManager._fetch_graphql_query(query, retries_count - 1, **kwargs)
        else:
            raise Exception(f"Query '{query}' failed to run by returning code of {res.status_code}: {res.json()}")

    @staticmethod
    def _find_pagination_and_data_list(response: Dict) -> Tuple[List, Dict]:
        if "nodes" in response.keys() and "pageInfo" in response.keys():
            return response["nodes"], response["pageInfo"]
        elif len(response) == 1 and isinstance(response[list(response.keys())[0]], Dict):
            return DownloadManager._find_pagination_and_data_list(response[list(response.keys())[0]])
        else:
            return list(), dict(hasNextPage=False)

    @staticmethod
    async def _fetch_graphql_paginated(query: str, **kwargs) -> Dict:
        initial_query_response = await DownloadManager._fetch_graphql_query(query, **kwargs, pagination="first: 100")
        page_list, page_info = DownloadManager._find_pagination_and_data_list(initial_query_response)
        while page_info["hasNextPage"]:
            pagination = f'first: 100, after: "{page_info["endCursor"]}"'
            query_response = await DownloadManager._fetch_graphql_query(query, **kwargs, pagination=pagination)
            new_page_list, page_info = DownloadManager._find_pagination_and_data_list(query_response)
            page_list += new_page_list
        return page_list

    @staticmethod
    async def get_remote_graphql(query: str, **kwargs) -> Dict:
        key = f"{query}_{md5(dumps(kwargs, sort_keys=True).encode('utf-8')).digest()}"
        if key not in DownloadManager._REMOTE_RESOURCES_CACHE:
            if "$pagination" in GITHUB_API_QUERIES[query]:
                res = await DownloadManager._fetch_graphql_paginated(query, **kwargs)
            else:
                res = await DownloadManager._fetch_graphql_query(query, **kwargs)
            DownloadManager._REMOTE_RESOURCES_CACHE[key] = res
        else:
            res = DownloadManager._REMOTE_RESOURCES_CACHE[key]
        return res
