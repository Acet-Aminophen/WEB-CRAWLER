package webCrawlerTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.StringReader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.SimpleDateFormat;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;

public class webCrawler
{
	private String getNow()
	{
		Date time = new Date();
		SimpleDateFormat format1 = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss");
		return format1.format(time);
	}
	private String load(String directory)
	{
		try {
			File chkFile = new File(directory);
			if (!chkFile.exists()) return "ERR|NO SUCH FILE DIRECTORY";
			
			FileInputStream input = new FileInputStream(directory);
			byte[] context = new byte[(int) chkFile.length()];
			input.read(context);
			input.close();
			return new String(context);
		} catch (Exception e) {
			return "ERR|" + e.toString();
		}
	}
	private String getValue(String directory, String value)
	{
		String str = load(directory);
		BufferedReader reader = new BufferedReader(new StringReader(str));
		String nLine = null;
		try
		{
			while( (nLine = reader.readLine()) != null )
			{
				if (nLine.contains(value)) { return nLine.split("\\|")[1]; }
			}
		}
		catch(Exception e) {}
		return "NOTHING";
	}
	private List<String> getValues(String directory, String value)
	{
		String str = load(directory);
		BufferedReader reader = new BufferedReader(new StringReader(str));
		String nLine = null;
		try
		{
			while( (nLine = reader.readLine()) != null )
			{
				if (nLine.contains(value))
				{
					return new ArrayList<String>(Arrays.asList(nLine.split("\\|")[1].split(","))) ;
				}
			}
		}
		catch(Exception e) {}
		return new ArrayList<String>();
	}
	// 멀티 thread라 생각
	class frontier
	{
		public frontier(String seed, int maxDepth, List<String> whitelist)
		{
			_mustVisitPages.add(seed); 
			_depthMustVisitPages.add(1);
			_maxDepth = maxDepth;
			_whitelist = whitelist;
		}
		
		private int _maxDepth = 0;
		private List<String> _whitelist = new ArrayList<String>();
		private List<String> _visitedPages = new ArrayList<String>();
		private List<String> _mustVisitPages = new ArrayList<String>();
		private List<Integer> _depthMustVisitPages = new ArrayList<Integer>();
		
//		synchronized public List<String> getVisitedPages() { return _visitedPages; }
//		synchronized public List<String> getMustVisitPages() { return _mustVisitPages; }
		// 위험
		synchronized public void addVisitedPage(String page) { _visitedPages.add(page); }
		synchronized public void addMustVisitPage(String page) { _mustVisitPages.add(page); }
		synchronized public void addMustVisitPages(List<String> pages) { _mustVisitPages.addAll(pages); }
		synchronized public void addDepthMustVisitPages(List<Integer> depths) { _depthMustVisitPages.addAll(depths); }
		
		synchronized public List<Object> getMusitVisitPage()
		{
			if (_mustVisitPages.isEmpty()) return new ArrayList<Object>();
			
			List<Object> msg = new ArrayList<Object>();
			String str = _mustVisitPages.get(0);
			_mustVisitPages.remove(0);
			addVisitedPage(str);
			
			Integer depth = _depthMustVisitPages.get(0);
			_depthMustVisitPages.remove(0);
			
			msg.add(str);
			msg.add(depth);
			
			System.out.println("방문함 : " + _visitedPages.size() + " 방문필 : " + _mustVisitPages.size());
			
			return msg;
		}
		
		public void doFilter(List<String> links, int depth)
		{
			if (depth > _maxDepth) return;
			
			links = links.stream().distinct().collect(Collectors.toList());
			//중복 제외
			for(int i = 0; i < links.size(); i++)
			{
				if (links.get(i).contains("#") || links.get(i).contains("javascript") || _visitedPages.contains(links.get(i)) || _mustVisitPages.contains(links.get(i)) )
				{
					links.set(i, null);
					continue;
				}
				//#이동 태그가 있거나, 함수거나, 방문한 또는 방문할 페이지에 있으면 제외
				
				boolean chkIsOnWhite = false;
				for (String white : _whitelist) if (links.get(i).contains(white)) chkIsOnWhite = true;
				if (!chkIsOnWhite) links.set(i, null);
				//화이트 리스트에 없을 경우 제외
			}
			while(links.remove(null));
			
			List<Integer> depths = new ArrayList<Integer>();
			for(int i = 0; i < links.size(); i++) depths.add(depth);
			
			//System.out.println(links.size() + " " + depths.size());
			
			addMustVisitPages(links);
			addDepthMustVisitPages(depths);
		}
	}
	
	class dbManager
	{
		Connection conn;
		public dbManager(String dbConnector, String dbType, String dbIp, String dbPort, String dbName, String dbId, String dbPwd)
		{
			try
			{
				conn = DriverManager.getConnection(dbConnector +":" + dbType + "://" + dbIp + ":" + dbPort + "/" + dbName, dbId, dbPwd);
			}catch(Exception e) { e.printStackTrace();}
		}
		public boolean insert(String sql)
		{
			try
			{
				Statement stmt = conn.createStatement();
				stmt.executeUpdate(sql);
				return true;
			}
			catch(Exception e)
			{
				e.printStackTrace();
				return false;
			}
		}
	}
	
	class agent
	{
		WebDriver _driver;
		String dbTable = "";
		
		
		public agent(String dbTable)
		{
			_driver = makeDriver(false);
			this.dbTable = dbTable;
			try
			{
				Class.forName("com.mariadb.jdbc.Driver");
			}catch(Exception e) { e.printStackTrace(); }
		}
		
		private WebDriver makeDriver(boolean headless)
		{
			System.setProperty("webdriver.chrome.driver", "C:\\selenium\\chromedriver.exe");
			if(!headless) return new ChromeDriver();
			else
			{
				ChromeOptions options = new ChromeOptions();
				options.addArguments("headless");
				return new ChromeDriver(options);
			}
		}
		private List<String> getHrefs()
		{
			List<WebElement> links = _driver.findElements(By.tagName("a"));
			List<String> strLinks = new ArrayList<String>();
			
			for (WebElement ele : links)
			{
				strLinks.add(ele.getAttribute("href"));
			}
			while(strLinks.remove(null));
			//도대체 어떻게 null값이 존재???
			
			return strLinks;
		}
		
		class inform
		{
			private String getTitle()
			{
				List<WebElement> ele = _driver.findElements(By.className("titSub"));
				if (!ele.isEmpty()) return ele.get(0).getText();
				
				ele = _driver.findElements(By.className("titMain"));
				if (!ele.isEmpty()) return ele.get(0).getText();
				return "";
			}
			private boolean isBoard()
			{
				if (!_driver.findElements(By.className("board_view")).isEmpty()) return true;
				else return false;
			}
			private String getAuthor()
			{
				List<WebElement> ele = _driver.findElements(By.name("author"));
				if (!ele.isEmpty()) return ele.get(0).getAttribute("content");
				else return "";
			}
			private String getBoardTitle()
			{
				List<WebElement> ele = _driver.findElement(By.className("board_header")).findElements(By.className("title"));
				if (!ele.isEmpty())
				{
					String str = ele.get(0).getText();
					if (str.length() > 40) str = str.substring(0,40);
					return str.replace("'", "''");
				}
				else return "";
			}
			private String getBoardContent()
			{
				List<WebElement> ele = _driver.findElement(By.className("board_body")).findElements(By.className("text-center"));
				if (!ele.isEmpty())
				{
					String str = ele.get(0).getText();
					if (str.length() > 4000) str = str.substring(0,4000);
					return str.replace("'", "''");
				}
				else return "";
			}
			private String getBoardDate()
			{
				return _driver.findElement(By.className("board_body")).findElement(By.className("data")).getText().substring(3);
			}
			public String getDwlQuery(String url, int depth)
			{
				String str = "INSERT INTO " + dbTable + "(address, depth, collectDate, isBoard";
				str += ") VALUES('" + url + "'," + Integer.toString(depth) + ",'" + getNow() + "', false)";
				return str;
			}
			public String getQuery(int depth)
			{
				boolean isBoard = isBoard();
				
				String str = "INSERT INTO " + dbTable + "(address, depth, title, collectDate, author, isBoard";
				
				if (isBoard) str += ", boardTitle, boardContent, boardDate";
				
				str += ") VALUES('" + _driver.getCurrentUrl() + "'," + Integer.toString(depth) + ",'" + getTitle() + "','" + getNow() + "','" + getAuthor() + "',";
				if(isBoard)
				{
					str += "true,'" + getBoardTitle() + "','" +getBoardContent() + "','" + getBoardDate() + "')";
				}
				else
				{
					str += "false)";
				}
				//System.out.println(str);
				return str;
			}
		}
		
		public void run(frontier frontier, dbManager dbManager)
		{
			try
			{
				List<Object> msg = frontier.getMusitVisitPage();
				String pastUrl = "";
				while(msg.size() > 0)
				{
					String url = msg.get(0).toString();
					int depth = (int)msg.get(1);
					
					_driver.get(url);
					Thread.sleep(1000);
					//트래픽 초과 방지
					String nowUrl = _driver.getCurrentUrl();
					
					if (nowUrl.equals(pastUrl))
					{
						dbManager.insert(new inform().getDwlQuery(url, depth));
						msg = frontier.getMusitVisitPage();
						continue;
					}
					else { pastUrl = nowUrl; }
					
					frontier.doFilter(getHrefs(), depth + 1);
					//수집된 모든 URL 프론티어로 전송
					dbManager.insert(new inform().getQuery(depth));
					//수집된 정보 dbManager로 전송
					
					msg = frontier.getMusitVisitPage();
					//프론티어에게 다음 방문 페이지 요청
				}
			}catch(Exception e) { e.printStackTrace(); }
			finally{ _driver.close(); }
			System.out.println("탐색 종료");
		}
	}
	
	public void run(String configDirectory)
	{
		String seed = getValue(configDirectory, "seed");
		int maxDepth = Integer.parseInt(getValue(configDirectory, "depth"));
		List<String> whitelist = getValues(configDirectory, "whitelist");
		String dbTable = getValue(configDirectory, "dbTable");
		String dbConnector = getValue(configDirectory, "dbConn");
		String dbType = getValue(configDirectory, "dbType");
		String dbIp = getValue(configDirectory, "dbIp");
		String dbPort = getValue(configDirectory, "dbPort");
		String dbName = getValue(configDirectory, "dbName");
		String dbId = getValue(configDirectory, "dbId");
		String dbPwd = getValue(configDirectory, "dbPwd");
		
		frontier frontier = new frontier(seed, maxDepth, whitelist);
		agent agent = new agent(dbTable);
		dbManager dbManager = new dbManager(dbConnector, dbType, dbIp, dbPort, dbName,dbId, dbPwd);
		
		agent.run(frontier, dbManager);
	}
}