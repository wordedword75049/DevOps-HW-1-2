import React, { Fragment, useEffect, useState } from 'react';
import Table from '@bostongene/frontend-components/Table';
import Tooltip from '@bostongene/frontend-components/Tooltip';
import { Link } from 'react-router-dom';
import DrugsListFilters, { IFilterData } from './blocks/DrugsListFilters';
import axios from 'axios'




const DrugsList = () => {
  const [filterData, setFilterData] = useState({ maxDrugPhase: '' });
  const [candidates, setCandidates] = useState([]);
  const onFilterDataChange = (data: IFilterData) => setFilterData(data);
  useEffect(() => {
    axios.get('/drugs-candidates?source=nci').then((({data}) => setCandidates(data.candidates)))
  }, [candidates])

  return (
    <Fragment>
      <DrugsListFilters onFilterDataChange={onFilterDataChange} />
      <Table
        items={candidates}          
        schema={[
          {
            name: '',
            template: ({ candidate, candidateId }) => (
              <div>
                <Link to={`/drugs/${candidateId}`}>{candidate}</Link>
              </div>
            ),
            className: 'col-xs-2',
            headerTemplate: field => (
              <div>
                {field.name} <Tooltip label="canidate" tooltipText="possible Candidate name" />
              </div>
            )
          },
          {
            name: '',
            template: ({ flag }) => (
              <div>
                <div>{flag}</div>
              </div>
            ),
            className: 'col-xs-2',
            headerTemplate: field => (
              <div>
                {field.name}{' '}
                <Tooltip
                  label="Flag"
                  tooltipText="A flag that shows if the drug either TP, FP, black_list or without a flag"
                />{' '}
              </div>
            )
          },
          {
            name: '',
            template: ({ nctCount }) => <div>{nctCount}</div>,
            className: 'col-xs-2',
            headerTemplate: field => (
              <div>
                {field.name}{' '}
                <Tooltip
                  label="NCT count"
                  tooltipText="Number of NCTs where candidate was found "
                />{' '}
              </div>
            )
          },
          {
            name: '',
            template: ({ avicennaCount }) => <div>{avicennaCount}</div>,
            className: 'col-xs-2',
            headerTemplate: field => (
              <div>
                {field.name}{' '}
                {/* <Tooltip
                  tooltipText={
                    <div style={{ display: 'inline', alignItems: 'center' }}>
                      <div
                        style={{
                          display: 'inline',
                          justifyContent: 'center',
                          alignItems: 'center',
                          minWidth: '20px',
                          minHeight: '20px',
                          color: '#ffffff',
                          borderRadius: '50%',
                          marginRight: '8px'
                        }}
                      >
                        1
                      </div>
                      <div>Number of matches in Avicenna drug dictionary</div>
                    </div>
                  }
                >
                  Found in Avicenna count
                </Tooltip> */}
                  <Tooltip label="Found in Avicenna count" tooltipText="Number of matches in Avicenna drug dictionary " />{' '}
              </div>
            )
          },

          {
            name: '',
            template: ({ maxPhase }) => (
              <div>
                <div>{maxPhase}</div>
              </div>
            ),
            className: 'col-xs-2',
            headerTemplate: field => (
              <div>
                {field.name}{' '}
                <Tooltip label="Max Phase" tooltipText="Max phase which was found in all NCTs " />{' '}
              </div>
            )
          },
          {
            name: '',
            template: ({ fdaLabelDate }) => (
              <div>
                <div>{fdaLabelDate}</div>
              </div>
            ),
            className: 'col-xs-2',
            headerTemplate: field => (
              <div>
                {field.name}{' '}
                <Tooltip
                  label="Last FDA Label Date"
                  tooltipText="The review date of the last FDA label"
                />{' '}
              </div>
            )
          }
        ]}
        rowHeightSize="medium"
      />
    </Fragment>
  );
};

export default DrugsList;
