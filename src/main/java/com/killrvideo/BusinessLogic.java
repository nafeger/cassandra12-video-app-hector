package com.killrvideo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;

import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

public class BusinessLogic {

	private static StringSerializer stringSerializer = StringSerializer.get();
	private static UUIDSerializer uuidSerializer = UUIDSerializer.get();

	public void setUser(User user, Keyspace keyspace) {
		StringSerializer se = StringSerializer.get();
		CqlQuery<String,String,String> cqlQuery = new CqlQuery<String,String,String>(keyspace, se, se, se);
		String cqlSource = "insert into users (username, firstname, lastname, password) values (''{0}'',''{1}'',''{2}'',''{3}'')";
		String cql = MessageFormat.format(cqlSource, q(user.getUsername()), q(user.getFirstname()),q( user.getLastname()), q(user.getPassword()));
		cqlQuery.setQuery(cql);
		cqlQuery.execute();
	}
	
	private static String q(String s) {
		return s.replace("'","''");
	}

	public User getUser(String username, Keyspace keyspace) {

		StringSerializer se = StringSerializer.get();
		CqlQuery<String,String,String> cqlQuery = new CqlQuery<String,String,String>(keyspace, se, se, se);
		String cqlSource = "select * from users where username = ''{0}''";
		String cql = MessageFormat.format(cqlSource, q(username));
		cqlQuery.setQuery(cql);
		QueryResult<CqlRows<String, String, String>> result = cqlQuery.execute();
		for (Row<String, String, String> x: result.get()) {
			ColumnSlice<String, String> columnSlice = x.getColumnSlice();
			HColumn<String, String> usernameCol = columnSlice.getColumnByName("username");
			HColumn<String, String> firstnameCol = columnSlice.getColumnByName("firstname");
			HColumn<String, String> lastnameCol = columnSlice.getColumnByName("lastname");
			HColumn<String, String> passwordCol = columnSlice.getColumnByName("password");
			User u = new User(usernameCol.getValue(), firstnameCol.getValue(), lastnameCol.getValue());
			u.setPasswordDigest(passwordCol.getValue());
			return u;
		}
		return null;
	}

	public void setVideo(final Video video, Keyspace keyspace) {

		withConnection(new IStatementActor() {

			public Statement act(Connection conn) throws SQLException {
				String query = "insert into videos (videoid, videoname, username, description, tags) values (?,?,?,?,?)";
				PreparedStatement statement = conn.prepareStatement(query);

				statement.setBytes(1, TimeUUIDUtils.asByteArray(video.getVideoId()));
				statement.setString(2, video.getVideoName());
				statement.setString(3, video.getUsername());
				statement.setString(4, video.getDescription());
				statement.setString(5, video.getDelimitedTags());

				statement.executeUpdate();
				return statement;
			}
		});

	}

	public Video getVideoByUUID(UUID videoId, Keyspace keyspace) {

		Video video = new Video();

		// Create a slice query. We'll be getting specific column names
		SliceQuery<UUID, String, String> sliceQuery = HFactory.createSliceQuery(keyspace, uuidSerializer,
				stringSerializer, stringSerializer);
		sliceQuery.setColumnFamily("videos");
		sliceQuery.setKey(videoId);

		sliceQuery.setColumnNames("videoname", "username", "description", "tags");

		// Execute the query and get the list of columns
		ColumnSlice<String, String> result = sliceQuery.execute().get();

		// Get each column by name and add them to our video object
		video.setVideoName(result.getColumnByName("videoname").getValue());
		video.setUsername(result.getColumnByName("username").getValue());
		video.setDescription(result.getColumnByName("description").getValue());
		video.setTags(result.getColumnByName("tags").getValue().split(","));

		return video;
	}

	public void setVideoWithTagIndex(Video video, Keyspace keyspace) {

		try {
			setVideo(video, keyspace);

			tagVideo(video, keyspace);

		} catch (HectorException he) {
			throw new RuntimeException(he);
		}

	}

	private void tagVideo(final Video video, final Keyspace keyspace) {
		for (final String tag : video.getTags()) {

			withConnection(new IStatementActor() {

				public Statement act(Connection conn) throws SQLException {
					String query = "insert into tag_index ( tag, videoid, info) values (?,?,?)";
					PreparedStatement statement = conn.prepareStatement(query);
					statement.setString(1, tag);
					statement.setBytes(2, TimeUUIDUtils.asByteArray(video.getVideoId()));
					statement.setString(3, makedate(new Date()));
					statement.executeUpdate();
					return statement;

				}
			});
		}
	}

	private void withConnection(IStatementActor actor) {
		Statement statement = null;
		Connection conn = null;
		try {
			Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
			conn = DriverManager.getConnection("jdbc:cassandra://localhost:9160/Killrvideo?version=3.0.0");

			statement = actor.act(conn);

		} catch (Exception e) {
			throw new RuntimeException(e);
	 	} finally {
	 		try {
	 			if (statement != null)
					statement.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	 		try {
	 			if (conn != null)
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	 	}
	}

	private String makedate(Date date) {
		SimpleDateFormat ISO8601DATEFORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		return ISO8601DATEFORMAT.format(date);
	}

	public void setVideoWithUserIndex(Video video, Keyspace keyspace) {
		// TODO Implement this method
		/*
		 * This mthod is similar to the setVideo but with a subtle twist. When
		 * you insert a new video, you will need to insert into the
		 * username_video_index at the same time for username to video lookups.
		 */

	}

	public void setComment(Video video, String comment, Timestamp timestamp, Keyspace keyspace) {

		Mutator<UUID> mutator = HFactory.createMutator(keyspace, UUIDSerializer.get());

		try {
			String columnName = video.getUsername() + ":" + timestamp;
			mutator.addInsertion(video.getVideoId(), "comments", HFactory.createStringColumn(columnName, comment));

			mutator.execute();
		} catch (HectorException he) {
			he.printStackTrace();
		}
	}

	public ArrayList<String> getComments(UUID videoId) {
		// TODO Implement
		/*
		 * Each video can have a unbounded list of comments associated with it.
		 * This method should return all comments associated with one video.
		 */

		return null;
	}

	public ArrayList<String> getCommentsOnTimeSlice(Timestamp startTimestamp, Timestamp stopTimestamp, UUID videoId) {
		// TODO Implement
		/*
		 * Each video can have a unbounded list of comments associated with it.
		 * This method should return comments from one timestamp to another.
		 */

		return null;
	}

	public void setRating(UUID videoId, long ratingNumber, Keyspace keyspace) {
		Mutator<UUID> mutator = HFactory.createMutator(keyspace, UUIDSerializer.get());

		try {

			mutator.addCounter(videoId, "video_rating", HFactory.createCounterColumn("rating_counter", 1));
			mutator.addCounter(videoId, "video_rating", HFactory.createCounterColumn("rating_total", ratingNumber));

			mutator.execute();
		} catch (HectorException he) {
			he.printStackTrace();
		}
	}

	public float getRating(UUID videoId) {
		// TODO Implement
		/*
		 * Each video has two things. a rating_count and rating_total. The
		 * average rating is calculated by taking the total and dividing by the
		 * count. Build the logic to get both numbers and return the average.
		 */

		return 0;
	}

	public void setVideoStartEvent(final UUID videoId, final String username, final Date date, Keyspace keyspace) {
		withConnection(new IStatementActor() {

			public Statement act(Connection conn) throws SQLException {
				String query = "insert into video_event ( videoid, username, event_type, event_date) values (?,?,?,?)";
				PreparedStatement statement = conn.prepareStatement(query);
				statement.setBytes(1, TimeUUIDUtils.asByteArray(videoId));
				statement.setString(2, username);
				statement.setString(3, "start");
				statement.setString(4, makedate(date));
				statement.executeUpdate();
				return statement;

			}
		});

	}

	public void setVideoStopEvent(final UUID videoId, final String username, final Date date, 
			final int positionInSeconds,
			Keyspace keyspace) {
		withConnection(new IStatementActor() {

			public Statement act(Connection conn) throws SQLException {
				String query = "insert into video_event ( videoid, username, event_type, event_date, extra_info) values (?,?,?,?,?)";
				PreparedStatement statement = conn.prepareStatement(query);
				statement.setBytes(1, TimeUUIDUtils.asByteArray(videoId));
				statement.setString(2, username);
				statement.setString(3, "stop");
				statement.setString(4, makedate(date));
				// this should be a real json thingy.
				statement.setString(5, "{ video_position_in_seconds: "+positionInSeconds+"}");
				statement.executeUpdate();
				return statement;

			}
		});
	}

	public Timestamp getVideoLastStopEvent(UUID videoId, String username) {
		// TODO Implement
		/*
		 * This method will return the video timestamp of the last stop event
		 * for a given video identified by videoid. As a hint, you will be using
		 * a getSlice to find certain strings to narrow the search.
		 */

		return null;
	}
}
