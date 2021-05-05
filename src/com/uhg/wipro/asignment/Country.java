package com.uhg.wipro.asignment;

public class Country {
	
	private String countryCode;
    private String countryName;
    private Long totalDownloads;
	public Country(String countryCode, String countryName, Long totalDownloads) {
		super();
		this.countryCode = countryCode;
		this.countryName = countryName;
		this.totalDownloads = totalDownloads;
	}
	public String getCountryCode() {
		return countryCode;
	}
	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}
	public String getCountryName() {
		return countryName;
	}
	public void setCountryName(String countryName) {
		this.countryName = countryName;
	}
	public Long getTotalDownloads() {
		return totalDownloads;
	}
	public void setTotalDownloads(Long totalDownloads) {
		this.totalDownloads = totalDownloads;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((countryCode == null) ? 0 : countryCode.hashCode());
		result = prime * result + ((countryName == null) ? 0 : countryName.hashCode());
		result = prime * result + ((totalDownloads == null) ? 0 : totalDownloads.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Country other = (Country) obj;
		if (countryCode == null) {
			if (other.countryCode != null)
				return false;
		} else if (!countryCode.equals(other.countryCode))
			return false;
		if (countryName == null) {
			if (other.countryName != null)
				return false;
		} else if (!countryName.equals(other.countryName))
			return false;
		if (totalDownloads == null) {
			if (other.totalDownloads != null)
				return false;
		} else if (!totalDownloads.equals(other.totalDownloads))
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "Country [countryCode=" + countryCode + ", countryName=" + countryName + ", totalDownloads="
				+ totalDownloads + "]";
	}
    
}
