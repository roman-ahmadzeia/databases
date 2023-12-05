import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MovieDB {
    private final String url = "jdbc:postgresql://localhost/MovieDB";
    private final String user = "postgres";
    private final String password = "Romancodes123!";

    /**
     * Connect to the PostgreSQL database
     *
     * @return a Connection object
     */

    // question 1
    public Connection connect() {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, user, password);
            System.out.println("Connected to PostgreSQL Database");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        return connection;
    }

    // question 2
    public void insertDataWithoutParameters() {

        try (Connection connection = connect()) {

            connection.createStatement().executeUpdate("INSERT INTO public.\"BirthLocation\" (country, city, state) VALUES ('USA', 'Los Angeles', 'CA')");
            connection.createStatement().executeUpdate("INSERT INTO public.\"Genre\" (type) VALUES ('Action')");
            connection.createStatement().executeUpdate("INSERT INTO public.\"University\" (name, is_private, color) VALUES ('XYZ University', true, 'Blue')");
            connection.createStatement().executeUpdate("INSERT INTO public.\"Department\" (\"id_University\", name) VALUES (1, 'Computer Science')");
            connection.createStatement().executeUpdate("INSERT INTO public.\"Movie\" (title, release_time, date, rating, budget, gross) " +
                    "VALUES ('Inception', '12:00:00', '2023-01-01', 9, 150000000, 825532764)");
            connection.createStatement().executeUpdate("INSERT INTO public.\"Director\" (first_name, surname, year_of_birth, \"id_BirthLocation\", \"id_Movie\", \"id_University\") " +
                    "VALUES ('Christopher', 'Nolan', 1970, 1, 1, 1)");
            connection.createStatement().executeUpdate("INSERT INTO public.\"Actor\" (first_name, surname, year_of_birth, \"id_BirthLocation\", eye_color) " +
                    "VALUES ('Leonardo', 'DiCaprio', 1974, 1, 'blue')");
            connection.createStatement().executeUpdate("INSERT INTO public.\"MovieActor\" (\"id_Movie\", \"id_Actor\") VALUES (1, 1)");
            connection.createStatement().executeUpdate("INSERT INTO public.\"MovieGenre\" (\"id_Movie\", \"id_Genre\") VALUES (1, 1)");
            connection.createStatement().executeUpdate("INSERT INTO public.\"Cinema\" (cinema_name, location, type) VALUES ('Cineplex', 'New York', 'Multiplex')");
            connection.createStatement().executeUpdate("INSERT INTO public.\"Ticket\" (price, \"id_Cinema\") VALUES (10.0, 1)");
            connection.createStatement().executeUpdate("INSERT INTO public.\"ShowTime\" (show_name, show_time, show_duration, \"id_Cinema_Ticket\", \"id_Movie\") " +
                    "VALUES ('Morning Show', '10:00:00', 120, 1, 1)");
            connection.createStatement().executeUpdate("INSERT INTO public.\"Award\" (award_name, \"id_Movie\") VALUES ('Best Picture', 1)");
            connection.createStatement().executeUpdate("INSERT INTO public.\"Category\" (category_name, \"award_id_Award\", \"id_Movie_Award\") VALUES ('Drama', 1, 1)");

            System.out.println("Data inserted successfully without using parameters.");
        } catch (SQLException e) {
            System.out.println("Error inserting data: " + e.getMessage());
        }
    }

    public void insertDataWithParameters() {
        try (Connection connection = connect()) {
            // Insert data into BirthLocation table with parameters
            String insertBirthLocation = "INSERT INTO public.\"BirthLocation\" (country, city, state) VALUES (?, ?, ?)";
            try (
                    PreparedStatement preparedStatement = connection.prepareStatement(insertBirthLocation)) {
                preparedStatement.setString(1, "Canada");
                preparedStatement.setString(2, "Toronto");
                preparedStatement.setString(3, "Ontario");
                preparedStatement.executeUpdate();
            }

            String insertGenre = "INSERT INTO public.\"Genre\" (type) VALUES (?)";
            try (PreparedStatement genreStatement = connection.prepareStatement(insertGenre)) {
                genreStatement.setString(1, "Adventure");
                genreStatement.executeUpdate();
            }

            String insertUniversity = "INSERT INTO public.\"University\" (name, is_private, color) VALUES (?, ?, ?)";
            try (PreparedStatement universityStatement = connection.prepareStatement(insertUniversity)) {
                universityStatement.setString(1, "University Of Toronto");
                universityStatement.setBoolean(2, false);
                universityStatement.setString(3, "Dark Blue");
                universityStatement.executeUpdate();
            }


            String insertDepartment = "INSERT INTO public.\"Department\" (\"id_University\", name) VALUES (?, ?)";
            try (PreparedStatement departmentStatement = connection.prepareStatement(insertDepartment)) {
                departmentStatement.setInt(1, 2);
                departmentStatement.setString(2, "Political Science");
                departmentStatement.executeUpdate();
            }

            // Insert data into University table with parameters



            // Insert data into Movie table with parameters
            String insertMovie = "INSERT INTO public.\"Movie\" (title, release_time, date, rating, budget, gross) VALUES (?, ?, ?, ?, ?, ?)";
            try (PreparedStatement movieStatement = connection.prepareStatement(insertMovie)) {
                movieStatement.setString(1, "Spider-Man");
                movieStatement.setTime(2, java.sql.Time.valueOf("12:00:00"));
                movieStatement.setDate(3, java.sql.Date.valueOf("2023-11-18"));
                movieStatement.setInt(4, 10);
                movieStatement.setFloat(5, 170000000);
                movieStatement.setFloat(6, 885532764);
                movieStatement.executeUpdate();
            }

            // Insert data into Director table with parameters
            String insertDirector = "INSERT INTO public.\"Director\" (first_name, surname, year_of_birth, \"id_BirthLocation\", \"id_Movie\", \"id_University\") VALUES (?, ?, ?, ?, ?, ?)";
            try (PreparedStatement directorStatement = connection.prepareStatement(insertDirector)) {
                directorStatement.setString(1, "Bobby");
                directorStatement.setString(2, "Dylan");
                directorStatement.setInt(3, 1986);
                directorStatement.setInt(4, 2);
                directorStatement.setInt(5, 2);
                directorStatement.setInt(6, 2);
                directorStatement.executeUpdate();
            }

            // Insert data into Actor table with parameters
            String insertActor = "INSERT INTO public.\"Actor\" (first_name, surname, year_of_birth, \"id_BirthLocation\", eye_color) VALUES (?, ?, ?, ?, ?)";
            try (PreparedStatement actorStatement = connection.prepareStatement(insertActor)) {
                actorStatement.setString(1, "Tom");
                actorStatement.setString(2, "Holland");
                actorStatement.setInt(3, 1995);
                actorStatement.setInt(4, 2);
                actorStatement.setString(5, "brown");
                actorStatement.executeUpdate();
            }

            // Insert data into Department table with parameters


            // Insert data into MovieActor table with parameters
            String insertMovieActor = "INSERT INTO public.\"MovieActor\" (\"id_Movie\", \"id_Actor\") VALUES (?, ?)";
            try (PreparedStatement movieActorStatement = connection.prepareStatement(insertMovieActor)) {
                movieActorStatement.setInt(1, 2);
                movieActorStatement.setInt(2, 2);
                movieActorStatement.executeUpdate();
            }

            // Insert data into Genre table with parameters

            // Retrieve the generated ID for subsequent use


            // Insert data into MovieGenre table with parameters
            String insertMovieGenre = "INSERT INTO public.\"MovieGenre\" (\"id_Movie\", \"id_Genre\") VALUES (?, ?)";
            try (PreparedStatement movieGenreStatement = connection.prepareStatement(insertMovieGenre)) {
                movieGenreStatement.setInt(1, 2);
                movieGenreStatement.setInt(2, 2);
                movieGenreStatement.executeUpdate();
            }


            // Insert data into Cinema table with parameters
            String insertCinema = "INSERT INTO public.\"Cinema\" (cinema_name, location, type) VALUES (?, ?, ?)";
            try (PreparedStatement cinemaStatement = connection.prepareStatement(insertCinema)) {
                cinemaStatement.setString(1, "Cineplex");
                cinemaStatement.setString(2, "Toronto");
                cinemaStatement.setString(3, "3D");
                cinemaStatement.executeUpdate();



                // Insert data into Ticket table with parameters
                String insertTicket = "INSERT INTO public.\"Ticket\" (price, \"id_Cinema\") VALUES (?, ?)";
                try (PreparedStatement ticketStatement = connection.prepareStatement(insertTicket)) {
                    ticketStatement.setFloat(1, 13);
                    ticketStatement.setInt(2, 2);
                    ticketStatement.executeUpdate();
                }

                // Insert data into ShowTime table with parameters
                String insertShowTime = "INSERT INTO public.\"ShowTime\" (show_name, show_time, show_duration, \"id_Cinema_Ticket\", \"id_Movie\") VALUES (?, ?, ?, ?, ?)";
                try (PreparedStatement showTimeStatement = connection.prepareStatement(insertShowTime)) {
                    showTimeStatement.setString(1, "The Morning Show");
                    showTimeStatement.setTime(2, java.sql.Time.valueOf("18:00:00"));
                    showTimeStatement.setInt(3, 120);
                    showTimeStatement.setInt(4, 2); // Assuming ticket ID 1
                    showTimeStatement.setInt(5, 2);
                    showTimeStatement.executeUpdate();
                }

                // Insert data into Award table with parameters
                String insertAward = "INSERT INTO public.\"Award\" (award_name, \"id_Movie\") VALUES (?, ?)";
                try (PreparedStatement awardStatement = connection.prepareStatement(insertAward)) {
                    awardStatement.setString(1, "Best Acting");
                    awardStatement.setInt(2, 2);
                    awardStatement.executeUpdate();


                    // Insert data into Category table with parameters
                    String insertCategory = "INSERT INTO public.\"Category\" (category_name, \"award_id_Award\", \"id_Movie_Award\") VALUES (?, ?, ?)";
                    try (PreparedStatement categoryStatement = connection.prepareStatement(insertCategory)) {
                        categoryStatement.setString(1, "Love");
                        categoryStatement.setInt(2, 2);
                        categoryStatement.setInt(3, 2);
                        categoryStatement.executeUpdate();
                    }

                }

            }

            System.out.println("Data inserted successfully with parameters.");
        } catch (SQLException e) {
            System.out.println("Error inserting data: " + e.getMessage());
        }
    }

    public void QueryMovieDatabaseNoParam(){
        try (Connection connection = connect()) {
            Statement statement = connection.createStatement();
            String queryWithoutParameters = "SELECT AVG(2023 - year_of_birth) AS average_age FROM public.\"Actor\"";
            ResultSet resultSet = statement.executeQuery(queryWithoutParameters);

            while (resultSet.next()) {
                System.out.println("Average Age of Actors: " + resultSet.getDouble("average_age"));
            }
            System.out.println("Data Queried successfully without using parameters.");

        } catch (SQLException e) {
            System.out.println("Error querying data: " + e.getMessage());
        }

    }

    public void QueryMovieDataBaseWithParam(){
        try(Connection connection = connect()){
            PreparedStatement preparedStatement1 = connection.prepareStatement("SELECT * FROM public.\"Director\" WHERE \"id_BirthLocation\" IN (SELECT id FROM " +
                    "public.\"BirthLocation\" WHERE country = ?)");
            PreparedStatement preparedStatement2 = connection.prepareStatement("SELECT COUNT(*) AS movie_count FROM public.\"MovieActor\" WHERE \"id_Actor\" =" +
                    " (SELECT id FROM public.\"Actor\" WHERE first_name = ? AND surname = ?)");
            PreparedStatement preparedStatement3 = connection.prepareStatement("SELECT COUNT(*) AS actor_count FROM public.\"Actor\" WHERE eye_color = ?");

            preparedStatement1.setString(1, ("Canada"));
            preparedStatement2.setString(1, "Brad");
            preparedStatement2.setString(2, "Pitt");
            preparedStatement3.setString(1, "green");



            ResultSet resultSet1 = preparedStatement1.executeQuery();
            ResultSet resultSet2 = preparedStatement2.executeQuery();
            ResultSet resultSet3 = preparedStatement3.executeQuery();

            System.out.println("Directors In Canada");
            while (resultSet1.next()) {
                System.out.println("Director Name: " + resultSet1.getString("first_name") + " " + resultSet1.getString("surname"));
                System.out.println("Year of Birth: " + resultSet1.getInt("year_of_birth"));
            }
            while (resultSet2.next()) {
                System.out.println("Number of Movies with Brad Pitt: " + resultSet2.getInt("movie_count"));

            }
            while (resultSet3.next()) {
                System.out.println("Number of Actors with Green Eye Color: " + resultSet3.getInt("actor_count"));

            }

            System.out.println("Data Queried successfully using parameters");
        } catch (SQLException e){
            System.out.println("Error querying data:" + e.getMessage());
        }

    }

    public static void main(String[] args) {

        MovieDB db = new MovieDB();
//        db.insertDataWithoutParameters();
//        db.insertDataWithParameters();
        db.QueryMovieDatabaseNoParam();
        db.QueryMovieDataBaseWithParam();

    }
}

