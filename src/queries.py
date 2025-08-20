GET_EVENT_ID = """
query GetEventId($slug: String) {
  event(slug: $slug) { id name }
}
"""

GET_SETS_PAGE = """
query EventSets($eventId: ID!, $page: Int!, $perPage: Int!) {
  event(id: $eventId) {
    sets(page: $page, perPage: $perPage, sortType: STANDARD) {
      pageInfo { totalPages }
      nodes { id }
    }
  }
}
"""

GET_SET_DETAIL = """
query SetDetail($setId: ID!) {
  set(id: $setId) {
    slots {
      entrant {
        participants { player { gamerTag prefix } }
      }
      standing { stats { score { value } } }
    }
  }
}
"""

DISCOVER_TOURNAMENTS = """
query BayNorCalTournaments($page: Int, $perPage: Int, $coordinates: String!, $radius: String!, $after: Timestamp, $before: Timestamp) {
  tournaments(
    query: {
      page: $page
      perPage: $perPage
      filter: { location: { distanceFrom: $coordinates, distance: $radius }, afterDate: $after, beforeDate: $before }
      sortBy: "startAt"
    }
  ) {
    nodes {
      id
      name
      city
      slug
      startAt
      events {
        slug
        numEntrants
        videogame { name }
      }
    }
  }
}
"""

EVENT_STANDINGS = """
query EventStandings($eventId: ID!, $page: Int!, $perPage: Int!) {
  event(id: $eventId) {
    id
    name
    standings(query: { perPage: $perPage, page: $page }) {
      nodes {
        placement
        entrant { id name }
      }
    }
  }
}
"""
