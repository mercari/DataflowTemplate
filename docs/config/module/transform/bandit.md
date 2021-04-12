# Bandit Transform Module

Using the simple [bandit algorithm](https://en.wikipedia.org/wiki/Multi-armed_bandit), this transform takes as input the number of times it is presented to the user and feedback from the user, and outputs information indicating which options should be presented to the user.

## Transform module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `bandit` |
| inputs | required | Array<String\> | Specify the names of the step from which you want to process the data, including the name of the transform. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## Bandit transform module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| armField | required | String | Specify the field name with the value of the selected arm. (The choices that are presented to the user in A/B testing are called `arms` in the context of the Bandit algorithm.) |
| countField | required | String | Specify the field name with the number of times the arm has been presented to the user. |
| rewardField | required | String | Specify the field name with the value of the reward to arm. |
| algorithm | optional | Enum | You can specify either `egreedy`,`softmax`,`ucb` or `ts` as bandit algorithm. The default and recommended one is `ts`. |
| targetFields | optional | Array<String\> | If you want to split the target into separate tests, such as by user or page, specify field names with a value for the split. |
| initialArms | optional | Array<String\> | Specify the name of a choice(arm) that is known in advance. If there is a arm that is not specified here, its value will also be automatically reflected. |
| intervalSeconds | optional | Integer | This transformation will group data together over a specified period of time in order to process it efficiently. Specify the period in seconds. |
| epsilon | optional | Float | This parameter is used when using the `egreedy` algorithm. Specify a value between 0 and 1 as the ratio between conjugation and search(more precisely, the ratio of search). Default is 0.1 |
| tau | optional | Float | This parameter is used when using the `softmax` algorithm. It specifies a value greater than 0 as the degree of conjugation and search(The larger the value, the more priority is given to search). Default is 0.2 |
| coolingRate | optional | Double | Specify the decay rate when you want the values of `epsilon` or `tau` to decay over time. Default is 1.0 (no decay). |


You can give the transform the number of times to show the user and the reward from the user at different times.
For example, you can send a message with a countField value of 1 and a rewardField value of 0 immediately after displaying a certain option(arm) to the user, and send a message with a countField value of 0 and a rewardField value of 1 when a reward (such as purchasing the displayed content) is received from the user.

## Output Schema

| field | type | description |
| --- | --- | --- |
| target | String | If the parameter `targetFields` is specified, the concatenated value of this specified fields. |
| selectedArm | String | The arm that should be selected at this time, as inferred by this module. You can also use the probability value of states to determine your own. |
| timestamp | Timestamp | The event time at which this output was computed. |
| algorithm | Enum | The algorithm used to compute this output. |
| states | Array<ArmState\> | The state of each arm used to infer the arm's selection. |

### ArmState Schema

| field | type | description |
| --- | --- | --- |
| arm | String | Name of arm. |
| count | Integer | Number of arm selections. |
| value | Float | Sample mean of that arm's reward. |
| probability | Float | Probability that the arm should be selected. This value depends on the algorithm you specify. |

## Related example config files

* [BigQuery to Bandit to BigQuery](../../../../examples/bigquery-to-bandit-to-bigquery.json)
* [Cloud PubSub(Json) to Bandit to Cloud PubSub(Json) and BigQuery](../../../../examples/pubsub-to-bandit-to-pubsub-bigquery.json)
