import React, { Fragment, useState, PureComponent } from 'react';
import Table from '@bostongene/frontend-components/Table';
import './Experimental.scss';
import axios from 'axios';
interface IDrugProps {
  drugId: string;
}

interface IDrugState {
  isLoaded: boolean;
}


class Experimental extends PureComponent<
  IDrugProps, IDrugState > {
    state = {
      candidate: {},
      isLoaded: false
    }

    componentDidMount(){
      const {drugId} = this.props
      axios.get(`/drugs-candidates/${drugId}`).then((({data}) => this.setState({candidate: data, isLoaded: true})))
    }
    render(){
      const {candidate, isLoaded} = this.state
      return isLoaded &&
          <div>
          <h1>"{candidate.candidateName}"</h1>
          <h2>Source information</h2>
          <Table
            items={candidate.sourceInformation}
            schema={[
              {
                name: 'Source ID',
                template: sourceInformation => (
                  <div>
                    <a href={`https://clinicaltrials.gov/ct2/show/${sourceInformation.nctId}`}>
                    <strong>{sourceInformation.nctId}</strong>
                    </a>
                  </div>
                ),
                className: 'col-xs-4'
              },
              {
                name: 'Source Title',
                template: sourceInformation => (
                  <div>
                    {sourceInformation.nctBriefTitle}
                  </div>
                ),      
                className: 'col-xs-4'
              },
              {
                name: 'Sentence',
                template: sourceInformation => (
                  <div>
                    {sourceInformation.nctSentence}
                  </div>
                ),
                className: 'col-xs-4'
              }
            ]}
          />
          <h2>NCI information</h2>
          <Table
        items={candidate.nciInformation}
        schema={[
          {
            name: 'FDA applications',
            template: ({ fdaApplications }) =>
            fdaApplications.some((element) => element.fdaApplicationNumber !== '') ? (
                <ul>
                  {fdaApplications.map(({drugName, fdaApplicationNumber, fdaLabelLink}, i) => (
                    <li key={i}>{fdaApplicationNumber ? (
                      <a href={fdaLabelLink} target="_blank" rel="noopener noreferrer">
                       {drugName} - {fdaApplicationNumber}
                      </a>
                    ) : null}{' '}
                    </li>
                  ))}
                </ul>
              ) : (
                <div>&mdash;</div>
              ),
            className: 'col-xs-3'
          },
          {
            name: 'Last FDA date', 
                        template: ({ fdaApplications }) =>
                        fdaApplications.some((element) => element.fdaLabelDate !== '') ? (
                <ul>
                  {fdaApplications.map(({fdaLabelDate}, i) => (
                    <li key={i}>{fdaLabelDate}</li>
                  ))}
                </ul>
              ) : (
                <div>&mdash;</div>
              ),
        className: 'col-xs-2 '
          },
          {
            name: 'Synonyms',
            template: ({ canonicalNameSynonyms }) =>
            canonicalNameSynonyms.some((element) => element.Term !== '') ? (
              <ul>
              {canonicalNameSynonyms.map(({Term, Source, Type}, i) => (
                <li key={i}>{Term}, Source - {Source}, Type - {Type}</li>
              ))}
            </ul>
          ) : (
            <div>&mdash;</div>
          ),
            className: 'col-xs-3'
          },
          {
            name: 'Canonical names',
            template: ({ canonicalName, thesaurusLink }) => (
              <div>
                {canonicalName}{' '}
                {thesaurusLink ? (
                  <a href={thesaurusLink} target="_blank" rel="noopener noreferrer">
                    Cancer.gov Link
                  </a>
                ) : null}
              </div>
            ),
            className: 'col-xs-4'
          },
        ]}
        rowHeightSize="medium"
      />
                <h2>Max drug phase in NCT:</h2>
          <Table
            items={candidate.maxDrugPhaseInformation}
            schema={[
            {
              name: 'NCT ID',
              template: maxDrugPhaseInformation => (
                <div>
                    <a href={`https://clinicaltrials.gov/ct2/show/${maxDrugPhaseInformation.nctId}`}>
                  <strong>{maxDrugPhaseInformation.nctId}</strong>
                  </a>
                </div>
              ),
              className: 'col-xs-4'
            },
            {
              name: 'Brief Title',
              template: maxDrugPhaseInformation => (
                <div>
                  {maxDrugPhaseInformation.nctBriefTitle}
                </div>
              ),     
              className: 'col-xs-4'
            },
            {
              name: 'Phase',
              template: maxDrugPhaseInformation => (
                <div>
                  {maxDrugPhaseInformation.nctMaxPhase}
                </div>
              ),
              className: 'col-xs-4'
            },
          ]}
        />
                    <h2>Clinical trials information</h2>
          <Table
            items={candidate.clinicalTrialsInformation}
            schema={[
              {
                name: 'NCT ID',
                template: clinicalTrialsInformation => (
                  <div>
                    <a href={`https://clinicaltrials.gov/ct2/show/${clinicalTrialsInformation.nctId}`}>
                    <strong>{clinicalTrialsInformation.nctId}</strong>
                    </a>
                  </div>
                ),
                className: 'col-xs-3'
              },
              {
                name: 'Title',
                template: clinicalTrialsInformation => (
                  <div>
                    {clinicalTrialsInformation.nctBriefTitle}
                  </div>
                ),      
                className: 'col-xs-3'
              },
              {
                name: 'Phase',
                template: clinicalTrialsInformation => (
                  <div>
                    {clinicalTrialsInformation.nctPhase}
                  </div>
                ),
                className: 'col-xs-3'
              },
              {
                name: 'Interventions',
                template: clinicalTrialsInformation => (
                  <div>
                    {clinicalTrialsInformation.nctInterventions}
                  </div>
                ),
                className: 'col-xs-3'
              }
            ]}
          /> 
          {candidate.foundInAvicennaInformation.count > 0 &&
            <div className="avicenna-block__label">  
              <h2>
            {candidate.foundInAvicennaInformation.drugName} is found in Avicenna!
              </h2>
            </div>

          }
          </div>    
    }
  }
export default Experimental;
