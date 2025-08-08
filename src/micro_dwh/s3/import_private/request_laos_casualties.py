

def get_reddit_response_no_auth(
        subreddit: str, listing: str, limit: int, timeframe: str, before: str=None, after: str=None,
) -> dict:
    """
    - simplest request, if breaks => add authorisation
    - to paginate in the past, use argument `after` and use according attribute of response
      (it is equal to the name of minimum/the most early thing at the current page)
    """

    start = time.time()
    base_url = f'https://www.reddit.com/r/{subreddit}/{listing}.json?limit={limit}&t={timeframe}'
    if before and after:
        raise ValueError(f'Specify either `before`={before} argument or `after`={after}, not both')
    elif before:
        base_url = base_url + f'&before={before}'
    elif after:
        base_url = base_url + f'&after={after}'
    else:
        pass

    request = requests.get(base_url, headers={'User-agent': 'my-script'})
    if request.status_code != 200:
        logger.error(f'An error occ: status_code={request.status_code}, reason={request.reason}')
        # note: not raises in runtime, have not found out reason yet
        raise Exception(f'An error occured: status_code={request.status_code}, reason={request.reason}')
    try:
        response = request.json()
