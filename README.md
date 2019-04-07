# NBA_bet

NBA_bet is a package for predicting NBA games against betting lines. It has two main behaviors: 
1. Scrape and store team statistics, game results, and betting lines.
2. Generate predictions for each NBA game, compare the prediction to the betting line, and store the results.

As of now, the model uses a linear regression based on the [Four Factors of Basketball Success](https://www.basketball-reference.com/about/factors.html) which encapsulates shooting, turnovers, rebounding, and free throws. Further, we include the opposing four factors, which are how a team's opponents perform on the four factors in aggregate. Thus, each team has eight variables, and the model uses sixteen variables (eight for each team) for each prediction. 

The target, Y, or dependent variable is home Margin of Victory (MOV). Away MOV is simply the inverse of home MOV. MOV is targeted because it provides an easy comparison with two types of betting lines, the spread and moneyline. Here's what the spread and moneyline might look like for a matchup between the Milwaukee Bucks and Atlanta Hawks:

Milwaukee Bucks:
1. Spread: -8
2. Moneyline: -350

Atlanta Hawks:
1. Spread: 8
2. Moneyline: 270

First, the spread attempts to guess the MOV between two teams. The Milwaukee Bucks spread of -8 indicates the betting line expects the Bucks to beat the Hawks by eight points. Or, the Bucks are "given" eight points. If one thinks the Bucks will beat the Hawks by more than eight points, they bet the Bucks. If one believes the Bucks will either win by less than eight points or lose, they bet the Hawks. Typically, spreads have symetric, or near-symetric, odds where picking the Bucks or the Hawks provides an equal return on a correct bet.

In comparison, the moneyline states the likelihood of a team winning or losing in terms of a monetary return. A negative moneyline, such as the Buck's -350, means one must put up $350 in order to win $100. A positive moneyline, such as the Hawk's 270, means a bet of $100 will return $270 if it is correct. 

To compare the model's predictions to the betting lines, we look at the prediction's distance from the betting line. But before comparison, we must determine if the data and model match the assumptions of regression. This requires a separate discussion, but one may use the project's graphing functions to evaluate the model. Graphs indicate the residuals are normal and homoscedastic, there do not appear to be severe outliers, and the variance inflation factors, which test for multicollinearity, are acceptable.* 

Thus, there is decent reason to trust the model is well calibrated. Or in other words, an outcome the model predicts to happen 70% of the time will indeed happen 70% of the time. 


Note: The variance inflation factor of the intercept is very high. I have no clue if this is bad or unexpected. Any advice on the consequences of this are appreciated.


## Usage



## Status: Minimum Viable Product

In its current status, the project operates as a concurrent whole. From the run directory, the user can either run the project at their leisure with run.all, or they can set the project to run daily with run.daily which schedules run.all to run each day. Either method builds all databases and tables, scrapes all data, and predicts all games. In this sense, it's a viable product.

The project is minimal because it doesn't do anything else. First, it contains no other functionality and/or no method for propogating functionality through the run directory at run time. For example, the regression model has graphing functions available, but the user cannot specify graph generation unless they run the regression script on its own. Second, the project only generates predictions with a linear model on a specific subset of data. A better product would allow the user to choose their desired data and model. Finally, the current product lacks infrastructure. There are no tests, and there is no setup.py. Thus, the project is not particularly resiliant to bugs nor reproducible for users across environments. 


## Author
Spencer Weston

personal website: [Crockpot Thoughts](https://crockpotthoughts.wordpress.com/)

## License
MIT
