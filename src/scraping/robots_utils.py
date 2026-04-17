from urllib.robotparser import RobotFileParser

def check_robots(base_url, path="/", user_agent="ResearchBot/1.0"):
    try:
        robots_url = base_url.rstrip("/") + "/robots.txt"
        rp = RobotFileParser()
        rp.set_url(robots_url)
        rp.read()
        full_url = base_url.rstrip("/") + path
        allowed = rp.can_fetch(user_agent, full_url)
        print(f"Checking robots.txt: {robots_url}")
        if allowed:
            print(f"Path '{path}' is allowed for {user_agent}")
        else:
            print(f"Path '{path}' is NOT allowed for {user_agent}")
        return allowed
    except Exception as e:
        print(f"Could not fetch robots.txt: {e}")
        return True