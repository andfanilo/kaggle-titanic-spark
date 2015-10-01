#VARIABLE DESCRIPTIONS

PassengerId   |  ID of passenger
Survived      |  Survival (0 = No; 1 = Yes)
Pclass        |  Passenger Class (1 = 1st; 2 = 2nd; 3 = 3rd)
Name          |  Name
Sex           |  Sex
Age           |  Age
Sibsp         |  Number of Siblings/Spouses Aboard
Parch         |  Number of Parents/Children Aboard
Ticket        |  Ticket Number
Fare          |  Passenger Fare
Cabin         |  Cabin
Embarked      |  Port of Embarkation (C = Cherbourg; Q = Queenstown; S = Southampton)

#SPECIAL NOTES

__Pclass__ is a proxy for socio-economic status (SES)
 1st ~ Upper; 2nd ~ Middle; 3rd ~ Lower

__Age__ is in Years; Fractional if Age less than One (1)
 If the Age is Estimated, it is in the form xx.5

With respect to the family relation variables (i.e. sibsp and parch)
some relations were ignored.  The following are the definitions used
for sibsp and parch.

__Sibling__:  Brother, Sister, Stepbrother, or Stepsister of Passenger Aboard Titanic

__Spouse__:   Husband or Wife of Passenger Aboard Titanic (Mistresses and Fiances Ignored)

__Parent__:   Mother or Father of Passenger Aboard Titanic

__Child__:    Son, Daughter, Stepson, or Stepdaughter of Passenger Aboard Titanic


Other family relatives excluded from this study include cousins,
nephews/nieces, aunts/uncles, and in-laws.  Some children travelled
only with a nanny, therefore parch=0 for them.  As well, some
travelled with very close friends or neighbors in a village, however,
the definitions do not support such relations.