from asyncio import Task, create_task
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
        waka_7days=f"https://wakapi.陈莹.我爱你/api/compat/wakatime/v1/users/current/stats/last_7_days?api_key={EM.WAKATIME_API_KEY}",
        waka_last_12_months=f"https://wakapi.陈莹.我爱你/api/compat/wakatime/v1/users/current/stats/last_12_months?api_key={EM.WAKATIME_API_KEY}",
        waka_all=f"https://wakapi.陈莹.我爱你/api/compat/wakatime/v1/users/current/all_time_since_today?api_key={EM.WAKATIME_API_KEY}",
        github_stats=f"https://github-contributions.vercel.app/api/v1/{user_login}",
    )


class DownloadManager:
    _client = AsyncClient(timeout=60.0)
    _REMOTE_RESOURCES_CACHE = dict()

    @staticmethod
    async def load_remote_resources(**resources: str):
        """
        Start background GET requests and store Tasks in the cache.
        Using create_task avoids storing raw coroutine objects that cannot be
        re-awaited and also prevents 'coroutine was never awaited' warnings.
        """
        for resource, url in resources.items():
            # create_task wraps the coroutine in a Task that can be awaited multiple times
            DownloadManager._REMOTE_RESOURCES_CACHE[resource] = create_task(
                DownloadManager._client.get(url)
            )

    @staticmethod
    async def close_remote_resources():
        """
        Cancel and await all running Tasks/Awaitables in the cache.
        Be defensive: a Task may have already completed with an exception.
        We catch and ignore exceptions here to avoid failing shutdown.
        """
        for resource in list(DownloadManager._REMOTE_RESOURCES_CACHE.values()):
            try:
                if isinstance(resource, Task):
                    # Cancel if still pending
                    if not resource.done():
                        resource.cancel()
                    try:
                        await resource
                    except Exception:
                        # swallow exceptions during shutdown (network errors, cancels, etc.)
                        pass
                elif isinstance(resource, Awaitable):
                    try:
                        await resource
                    except Exception:
                        pass
            except Exception:
                # extra defensive guard: shouldn't normally happen
                pass

    @staticmethod
    async def _get_remote_resource(
        resource: str, convertor: Optional[Callable[[bytes], Dict]]
    ) -> Dict or None:
        DBM.i(f"\tMaking a remote API query named '{resource}'...")
        cached = DownloadManager._REMOTE_RESOURCES_CACHE[resource]
        # If the cache entry is an awaitable (Task), await it and then replace cache with the Response
        if isinstance(cached, Awaitable):
            try:
                res = await cached
                # replace the Task/Awaitable in cache with the resolved Response
                DownloadManager._REMOTE_RESOURCES_CACHE[resource] = res
                DBM.g(f"\tQuery '{resource}' finished, result saved!")
            except Exception as e:
                # Network/connect errors may happen; log and re-raise so caller can decide.
                DBM.w(f"\tQuery '{resource}' failed while awaiting remote request: {e}")
                raise

        else:
            # cached is already a response-like object
            res = cached
            DBM.g(f"\tQuery '{resource}' loaded from cache!")

        if res.status_code == 200:
            return res.json() if convertor is None else convertor(res.content)
        elif res.status_code in (201, 202):
            DBM.w(f"\tQuery '{resource}' returned {res.status_code} status code")
            return None
        else:
            # Keep behaviour consistent: surface the response body for debugging
            raise Exception(
                f"Query '{res.url}' failed to run by returning code of {res.status_code}: {res.json()}"
            )

    @staticmethod
    async def get_remote_json(resource: str) -> Dict or None:
        return await DownloadManager._get_remote_resource(resource, None)

    @staticmethod
    async def get_remote_yaml(resource: str) -> Dict or None:
        return await DownloadManager._get_remote_resource(resource, safe_load)

    @staticmethod
    async def _fetch_graphql_query(
        query: str, retries_count: int = 10, **kwargs
    ) -> Dict:
        headers = {"Authorization": f"Bearer {EM.GH_TOKEN}"}
        res = await DownloadManager._client.post(
            "https://api.github.com/graphql",
            json={"query": Template(GITHUB_API_QUERIES[query]).substitute(kwargs)},
            headers=headers,
        )
        if res.status_code == 200:
            return res.json()
        elif res.status_code == 502 and retries_count > 0:
            return await DownloadManager._fetch_graphql_query(
                query, retries_count - 1, **kwargs
            )
        else:
            raise Exception(
                f"Query '{query}' failed to run by returning code of {res.status_code}: {res.json()}"
            )

    @staticmethod
    def _find_pagination_and_data_list(response: Dict) -> Tuple[List, Dict]:
        """
        Traverse a nested GraphQL response dict and find the first dict that
        contains both 'nodes' and 'pageInfo'. Return (nodes, pageInfo).
        If not found, return ([], {'hasNextPage': False}).

        This is more robust than assuming intermediate dicts have only one key,
        because some nodes (e.g. 'target') may contain multiple keys like
        '__typename' and 'history'.
        """
        if not isinstance(response, dict):
            return list(), dict(hasNextPage=False)

        # Direct hit
        if "nodes" in response and "pageInfo" in response:
            return response["nodes"], response["pageInfo"]

        # Recurse into dictionary values
        for value in response.values():
            if isinstance(value, dict):
                nodes, page_info = DownloadManager._find_pagination_and_data_list(value)
                if nodes or page_info.get("hasNextPage", False):
                    return nodes, page_info
            elif isinstance(value, list):
                # Some GraphQL responses might embed lists; check each element
                for item in value:
                    if isinstance(item, dict):
                        nodes, page_info = DownloadManager._find_pagination_and_data_list(item)
                        if nodes or page_info.get("hasNextPage", False):
                            return nodes, page_info

        # Fallback: not found
        return list(), dict(hasNextPage=False)

    @staticmethod
    async def _fetch_graphql_paginated(query: str, **kwargs) -> Dict:
        initial_query_response = await DownloadManager._fetch_graphql_query(
            query, **kwargs, pagination="first: 100"
        )
        page_list, page_info = DownloadManager._find_pagination_and_data_list(
            initial_query_response
        )
        while page_info["hasNextPage"]:
            pagination = f'first: 100, after: "{page_info["endCursor"]}"'
            query_response = await DownloadManager._fetch_graphql_query(
                query, **kwargs, pagination=pagination
            )
            new_page_list, page_info = DownloadManager._find_pagination_and_data_list(
                query_response
            )
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
