package backend;

public class URLWithScores implements Comparable<Object> {
	private String url;
	private Integer searchTermScore;
	private Double rankingScore;

	public URLWithScores(String url, Integer searchTermScore, Double rankingScore) {
		this.url = url;
		this.searchTermScore = searchTermScore;
		this.rankingScore = rankingScore;
	}

	@Override
	public int compareTo(Object other) {
		int i = this.searchTermScore.compareTo(((URLWithScores) other).searchTermScore);
		if (i != 0)
			return -i;

		i = this.rankingScore.compareTo(((URLWithScores) other).rankingScore);
		if (i != 0)
			return -i;

		return this.url.compareTo(((URLWithScores) other).url);
	}

	public String getURL() {
		return this.url;
	}

	public Double getRankingScore() {
		return this.rankingScore;
	}

	public Integer getSearchTermScore() {
		return this.searchTermScore;
	}
}
