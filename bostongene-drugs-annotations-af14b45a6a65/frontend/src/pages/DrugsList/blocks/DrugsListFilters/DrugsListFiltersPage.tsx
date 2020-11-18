import React from 'react';
import Select, { ISelectTarget } from '@bostongene/frontend-components/Select';
import { IFilterData } from '.';
import './DrugsListFilters.scss';

interface IDrugsListFiltersPageProps {
  filterData: IFilterData;
  onPhaseChange: (data: ISelectTarget) => void;
  onCountChange: (data: ISelectTarget) => void;
}

const DrugsListFiltersPage = ({ filterData, onPhaseChange, onCountChange }: IDrugsListFiltersPageProps) => (
  <div>
    <div className="drugs-list-filters__label_1">Max drug phase</div>
    <div>
      <Select
        name="maxDrugPhase"
        options={[
          { label: 'All', value: '' },
          { label: 'Phase 4', value: 'Phase 4' },
          { label: 'Phase 3', value: 'Phase 3' },
          { label: 'Phase 2/Phase 3', value: 'Phase 2/Phase 3' },
          { label: 'Phase 2', value: 'Phase 2' },
          { label: 'Phase 1/Phase 2', value: 'Phase 1/Phase 2' },
          { label: 'Phase 1', value: 'Phase 1' },
          { label: 'Early Phase 1', value: 'Early Phase 1' },
          { label: 'N/A', value: 'N/A' }
        ]}
        value={filterData.maxDrugPhase}
        onChange={({ name, value }) => onPhaseChange({ name, value })}
      />
    </div>
    <div className="drugs-list-filters__label_2">Found in Avicenna</div>
    <div>
      <Select
        name="foundInAvicenna"
        options={[
          { label: 'All', value: '' },
          // { label: 'Phase 1/Phase 2', value: 'Phase 1/Phase 2' },
          { label: '0', value: '0'},
          { label: '1', value: '1' },
          // { label: 'Early Phase 1', value: 'Early Phase 1' },
          { label: 'N/A', value: 'N/A' }
        ]}
        value={filterData.maxDrugPhase}
        onChange={({ name, value }) => onCountChange({ name, value })}
      />
    </div>
  </div>
);

export default DrugsListFiltersPage;

// export default function DrugsListFiltersPage([
//   onPhaseChange
// ]) {
//   const renderPhaseField = (label, fieldName) => (
//     <div className="candidate-filter__field row">
//       <div className="col-xs-12">
//         <label className="candidate-filter__label">{label}</label>
//       </div>
//       <div className="col-xs-12 annotations-filter__date-picker">
//           onChange={date =>
//             onChange({
//               name: fieldName,
//               value: date instanceof Date ? dayjs(date).format('YYYY-MM-DD') : ''
//             })
//           }
//         />
//       </div>
//     </div>
